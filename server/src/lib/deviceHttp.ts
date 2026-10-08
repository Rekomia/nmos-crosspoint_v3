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
/** How long an address that refused or never answered a connect is skipped. */
const UNREACHABLE_MS = 15000;

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

const queues:Map<string, Promise<void>> = new Map();
const unreachableUntil:Map<string, { until:number, code:string }> = new Map();

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

/**
 * One request to a device: waits for the device's earlier requests, then
 * runs on a fresh connection. An address that just refused or did not answer
 * a connect fails straight away for a few seconds instead of costing the
 * connect timeout again for every request queued behind it.
 */
export function deviceRequest<T = any>(queueKey:string, config:AxiosRequestConfig):Promise<AxiosResponse<T>>{
    const previous = queues.get(queueKey) || Promise.resolve();
    const run = previous.then(async () => {
        const origin = originOf(config.url || "");
        const down = unreachableUntil.get(origin);
        if(down && down.until > Date.now()){
            throw new AxiosError(origin + " did not take a connection " + Math.round((Date.now() - (down.until - UNREACHABLE_MS)) / 1000) +
                " s ago (" + down.code + ") — skipped", down.code, config as any);
        }else if(down){
            unreachableUntil.delete(origin);
        }
        try{
            const response = await axios.request<T>({ ...config, httpAgent, httpsAgent });
            unreachableUntil.delete(origin);
            return response;
        }catch(e){
            if(neverConnected(e)){
                unreachableUntil.set(origin, { until: Date.now() + UNREACHABLE_MS, code: (e as any).code });
            }else{
                unreachableUntil.delete(origin);
            }
            throw e;
        }
    });
    const tail = run.then(() => {}, () => {});
    queues.set(queueKey, tail);
    tail.then(() => { if(queues.get(queueKey) === tail){ queues.delete(queueKey); } });
    return run;
}
