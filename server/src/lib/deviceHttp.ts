/*
    NMOS Crosspoint
    Copyright (C) 2021 Johannes Grieb
*/

import * as http from "http";
import * as https from "https";
import axios, { AxiosError, AxiosRequestConfig, AxiosResponse } from "axios";

// ----- HTTP requests to NMOS devices -----
// Every request to a device (IS-05 connection API, IS-08, the senders' SDP
// manifests) goes through here. Registry requests do not.
//
// Embedded web servers handle fewer simultaneous requests than a device-level
// take sends them. A take onto the six receivers of a Riedel FusioN fired six
// GET /active and then six PATCH /staged at once; four PATCHes came back as
// "socket hang up". The device applied them and dropped the answer. Two rules
// fix that:
//
//  - One request at a time per device. The queue is keyed by the device's
//    NMOS node, not by the device: a FusioN publishes its senders and
//    receivers as separate devices behind one web server, and both of its
//    control addresses reach that same server.
//
//  - A new connection for every request. Node 19 and later keep connections
//    open between requests, and some devices close their end right after the
//    answer without saying so. The next request on that socket is lost with
//    "socket hang up". Serializing makes this worse, because the next request
//    always picks up the socket the last one just freed.

/** Longest a TCP connect to a device may take. On a LAN it takes milliseconds. */
const CONNECT_TIMEOUT_MS = 3000;
/**
 * How long an address is skipped after it refused a connection, did not take
 * one, or took one and never answered. Serialized, every request queued
 * behind such an address would otherwise wait out its own timeout: a device
 * that accepts connections and then stays silent turned a six-receiver take
 * from 40 s into 270 s.
 */
const SKIP_MS = 15000;

// Errors raised before the request reached the device: retrying elsewhere
// cannot apply anything twice.
const CONNECT_ERRORS = new Set(["ECONNREFUSED", "EHOSTUNREACH", "ENETUNREACH", "EHOSTDOWN", "ENOTFOUND", "EAI_AGAIN", "ETIMEDOUT"]);

function withConnectTimeout<T extends http.Agent>(agent:T):T{
    const create = (agent as any).createConnection.bind(agent);
    (agent as any).createConnection = (options:any, callback:any) => {
        const socket = create(options, callback);
        if(socket && typeof socket.once === "function"){
            const timer = setTimeout(() => {
                const err:any = new Error("connect to " + options.host + ":" + options.port + " timed out after " + CONNECT_TIMEOUT_MS + " ms");
                err.code = "ETIMEDOUT";
                socket.destroy(err);
            }, CONNECT_TIMEOUT_MS);
            socket.once("connect", () => clearTimeout(timer));
            socket.once("close", () => clearTimeout(timer));
        }
        return socket;
    };
    return agent;
}

// keepAlive:false with the default (unlimited) maxSockets makes Node send
// "Connection: close" and never reuse a socket. A finite maxSockets would
// bring keep-alive back.
const httpAgent = withConnectTimeout(new http.Agent({ keepAlive: false }));
const httpsAgent = withConnectTimeout(new https.Agent({ keepAlive: false }));

// Per queue key: whether a request is running, and what waits. Takes go
// before background reads: an IS-04 update of a large node queues a manifest
// and an /active read for every one of its senders, and a click on the
// matrix must not wait behind all of them.
interface Lane { busy:boolean, now:(() => Promise<void>)[], later:(() => Promise<void>)[] }
const lanes:Map<string, Lane> = new Map();
const skipUntil:Map<string, { until:number, code:string, what:string }> = new Map();

function originOf(url:string):string{
    try{
        return new URL(url).origin;
    }catch(e){
        return url;
    }
}

/**
 * The queue a device's requests wait in: its node when known, otherwise the
 * host of the URL.
 */
export function deviceQueueKey(device:any, url:string):string{
    if(device && device.node_id){
        return "node:" + device.node_id;
    }
    try{
        return "host:" + new URL(url).hostname;
    }catch(e){
        return "host:" + url;
    }
}

/** True when the request never reached the device. */
export function neverConnected(e:any):boolean{
    return !!e && !e.response && CONNECT_ERRORS.has(e.code);
}

/** True when the device got the connection but sent no answer back. */
export function noAnswer(e:any):boolean{
    return !!e && axios.isAxiosError(e) && !e.response && !neverConnected(e) && e.code !== "ERR_CANCELED";
}

function pump(queueKey:string, lane:Lane){
    if(lane.busy){ return; }
    const next = lane.now.shift() || lane.later.shift();
    if(!next){
        lanes.delete(queueKey);
        return;
    }
    lane.busy = true;
    next().then(() => {
        lane.busy = false;
        pump(queueKey, lane);
    });
}

async function send<T>(config:AxiosRequestConfig):Promise<AxiosResponse<T>>{
    const origin = originOf(config.url || "");
    const skip = skipUntil.get(origin);
    if(skip && skip.until > Date.now()){
        throw new AxiosError(origin + " " + skip.what + " " + Math.round((Date.now() - (skip.until - SKIP_MS)) / 1000) +
            " s ago (" + skip.code + ") — skipped", skip.code, config as any);
    }else if(skip){
        skipUntil.delete(origin);
    }
    try{
        const response = await axios.request<T>({ ...config, httpAgent, httpsAgent });
        skipUntil.delete(origin);
        return response;
    }catch(e){
        const code = (e as any)?.code;
        if(neverConnected(e)){
            skipUntil.set(origin, { until: Date.now() + SKIP_MS, code, what: "did not take a connection" });
        }else if(code === "ECONNABORTED" && !(e as any).response){
            // axios' own timeout: connected, but no answer in time.
            skipUntil.set(origin, { until: Date.now() + SKIP_MS, code, what: "left a request unanswered" });
        }else{
            skipUntil.delete(origin);
        }
        throw e;
    }
}

/**
 * One request to a device: waits for the device's earlier requests, then
 * runs on a fresh connection. `background` requests (reads nobody is waiting
 * for) let every take request queued for the device go first. An address
 * that just refused a connection or left a request unanswered fails straight
 * away for a few seconds instead of costing the timeout again for every
 * request queued behind it.
 */
export function deviceRequest<T = any>(queueKey:string, config:AxiosRequestConfig, background = false):Promise<AxiosResponse<T>>{
    return new Promise<AxiosResponse<T>>((resolve, reject) => {
        let lane = lanes.get(queueKey);
        if(!lane){
            lane = { busy:false, now:[], later:[] };
            lanes.set(queueKey, lane);
        }
        (background ? lane.later : lane.now).push(() => send<T>(config).then(resolve, reject));
        pump(queueKey, lane);
    });
}
