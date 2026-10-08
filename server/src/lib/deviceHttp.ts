/*
    NMOS Crosspoint
    Copyright (C) 2021 Johannes Grieb
*/

import * as http from "http";
import * as https from "https";
import axios, { AxiosError, AxiosRequestConfig, AxiosResponse } from "axios";
import { SyncLog } from "./syncLog";

// ----- HTTP requests to NMOS devices -----
// Every request to a device (IS-05 connection API, IS-08, the senders' SDP
// manifests) goes through here. Registry requests do not.
//
// Embedded web servers handle fewer simultaneous requests than a device-level
// take sends them. A take onto the six receivers of a Riedel FusioN fired six
// GET /active and then six PATCH /staged at once; four PATCHes came back as
// "socket hang up", yet all six receivers ended up patched. So:
//
//  - Requests to one device wait in one queue, keyed by its NMOS node: a
//    FusioN publishes its senders and receivers as separate devices behind
//    one web server, reachable on two addresses.
//
//  - A few run at once. A node that drops a connection while it has several
//    requests in hand gets one at a time from then on (until restart), and
//    what it dropped is sent again where that is safe. Strictly one at a
//    time for every device would make a take onto a large gateway many
//    times slower for no gain.
//
//  - A new connection for every request. Node 19 and later keep connections
//    open between requests, and some devices close their end right after the
//    answer without saying so. The next request on that socket is lost with
//    "socket hang up" — and queueing makes that more likely, because the
//    next request picks up the socket the last one just freed.
//
//  - Take requests go before background reads: one IS-04 update of a large
//    node queues an SDP and an /active read for every one of its senders,
//    and a click on the matrix must not wait behind them.

/** Requests one node gets at once until it shows it cannot take them. */
const PARALLEL = 4;
/** Longest a TCP connect to a device may take. On a LAN it takes milliseconds. */
const CONNECT_TIMEOUT_MS = 3000;
/**
 * How long an address that refused or did not take a connection is skipped
 * (and how long reads are skipped after one went unanswered). Every request
 * queued behind such an address would otherwise wait out its own timeout:
 * a device that accepts connections and stays silent turned a six-receiver
 * take from 40 s into 270 s.
 */
const SKIP_MS = 15000;

// Errors raised before the request reached the device. ESKIPPED is ours:
// the request was not sent at all.
const CONNECT_ERRORS = new Set(["ECONNREFUSED", "EHOSTUNREACH", "ENETUNREACH", "EHOSTDOWN", "ENOTFOUND", "EAI_AGAIN", "ETIMEDOUT", "ESKIPPED"]);
// The device closed the connection without an answer.
const RESET_ERRORS = new Set(["ECONNRESET", "EPIPE"]);

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

export interface DeviceRequestOptions {
    /** A read nobody is waiting for: the device's take requests go first. */
    background?: boolean;
    /** Sent even while the address is being skipped. */
    force?: boolean;
    /**
     * A read with no fallback (a sender's SDP): not held back by a skip that
     * only covers reads — a slow /active read elsewhere on the device says
     * nothing about it.
     */
    noFallback?: boolean;
    /**
     * Safe to send twice: sent again once when the device dropped it while
     * busy with others. GETs always are.
     */
    idempotent?: boolean;
}

/**
 * Sends one request from inside withDevice(). A failure it would have sent
 * again outside (refused while the device was busy) carries `resend: true`.
 */
export type DeviceSend = (config:AxiosRequestConfig, opts?:DeviceRequestOptions) => Promise<AxiosResponse>;

type Task = () => Promise<void>;
interface Lane { running:number, now:Task[], later:Task[] }
const lanes:Map<string, Lane> = new Map();
const serialNodes:Set<string> = new Set();

// "all": nothing goes out. "reads": GETs with a fallback stay back. "writes":
// PATCHes stay back — the device answers reads but hangs on activations.
interface Skip { until:number, code:string, what:string, kind:"all"|"reads"|"writes" }
const originSkips:Map<string, Skip> = new Map();
const nodeSkips:Map<string, Skip> = new Map();
// When each address last answered anything, error statuses included.
const lastAnswer:Map<string, number> = new Map();
/** A refused connection counts as overload only from an address that answered this recently. */
const ALIVE_MS = 2000;

function originOf(url:string):string{
    try{
        return new URL(url).origin;
    }catch(e){
        return url;
    }
}

function methodOf(config:AxiosRequestConfig):string{
    return (config.method || "get").toLowerCase();
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

/** True when the device got the request but sent no answer back. */
export function noAnswer(e:any):boolean{
    return !!e && axios.isAxiosError(e) && !e.response && !neverConnected(e) && e.code !== "ERR_CANCELED";
}

/**
 * Skip this node's requests for a while: it left a PATCH unanswered. With
 * `writesOnly` (it still answers reads) only its PATCHes stay back, until
 * one of them gets an answer again.
 */
export function markUnresponsive(queueKey:string, what:string, writesOnly = false){
    nodeSkips.set(queueKey, { until: Date.now() + SKIP_MS, code: "ECONNABORTED", what, kind: writesOnly ? "writes" : "all" });
}

function laneOf(queueKey:string):Lane{
    let lane = lanes.get(queueKey);
    if(!lane){
        lane = { running: 0, now: [], later: [] };
        lanes.set(queueKey, lane);
    }
    return lane;
}

function busy(queueKey:string):number{
    return lanes.get(queueKey)?.running || 0;
}

function pump(queueKey:string){
    const lane = lanes.get(queueKey);
    if(!lane){ return; }
    const limit = serialNodes.has(queueKey) ? 1 : PARALLEL;
    while(lane.running < limit){
        const next = lane.now.shift() || lane.later.shift();
        if(!next){ break; }
        lane.running++;
        next().then(() => {
            lane.running--;
            pump(queueKey);
        });
    }
    if(lane.running === 0 && lane.now.length === 0 && lane.later.length === 0){
        lanes.delete(queueKey);
    }
}

function enqueue(queueKey:string, task:Task, background:boolean, front = false){
    const lane = laneOf(queueKey);
    const list = background ? lane.later : lane.now;
    if(front){
        list.unshift(task);
    }else{
        list.push(task);
    }
    pump(queueKey);
}

function activeSkip(queueKey:string, origin:string, method:string, noFallback = false):{ skip:Skip, node:boolean }|null{
    const now = Date.now();
    for(const [map, key, node] of [[nodeSkips, queueKey, true], [originSkips, origin, false]] as [Map<string, Skip>, string, boolean][]){
        const skip = map.get(key);
        if(!skip){ continue; }
        if(skip.until <= now){
            map.delete(key);
            continue;
        }
        if(skip.kind === "all" ||
           (skip.kind === "reads" && method === "get" && !noFallback) ||
           (skip.kind === "writes" && method !== "get")){
            return { skip, node };
        }
    }
    return null;
}

/** The device answered: lift what this answer disproves. */
function answered(queueKey:string, origin:string, method:string){
    lastAnswer.set(origin, Date.now());
    originSkips.delete(origin);
    // A read answered says nothing about PATCHes that hang.
    if(method !== "get" || nodeSkips.get(queueKey)?.kind !== "writes"){
        nodeSkips.delete(queueKey);
    }
}

async function attempt(queueKey:string, config:AxiosRequestConfig, opts:DeviceRequestOptions):Promise<AxiosResponse>{
    const origin = originOf(config.url || "");
    const method = methodOf(config);
    if(!opts.force){
        const hit = activeSkip(queueKey, origin, method, !!opts.noFallback);
        if(hit){
            const ago = Math.round((Date.now() - (hit.skip.until - SKIP_MS)) / 1000);
            const err:any = new AxiosError("not sent: " + (hit.node ? "the device " : origin + " ") + hit.skip.what + " " + ago +
                " s ago (" + hit.skip.code + ")", "ESKIPPED", config as any);
            err.retryAt = hit.skip.until;
            throw err;
        }
    }
    try{
        const response = await axios.request({ ...config, httpAgent, httpsAgent });
        answered(queueKey, origin, method);
        return response;
    }catch(e:any){
        if(e?.response){
            // An error status is an answer: the device is there.
            answered(queueKey, origin, method);
        }
        throw e;
    }
}

/**
 * What a failure says about the device. Arms the skips and switches a node
 * that dropped requests under load to one at a time. Returns true when the
 * request should simply be sent again.
 */
function learn(queueKey:string, config:AxiosRequestConfig, opts:DeviceRequestOptions, e:any, crowded:boolean):boolean{
    if(!e || e.response || e.code === "ESKIPPED"){ return false; }
    const origin = originOf(config.url || "");
    const method = methodOf(config);
    const code = e.code;
    // Overloaded, not gone: it dropped this while busy with others. A reset
    // connection was accepted first, so the device is up. A refused one
    // counts only from an address that has just been answering — a device
    // that is rebooting, or an address nothing listens on, has not.
    const alive = (lastAnswer.get(origin) || 0) > Date.now() - ALIVE_MS;
    if(crowded && (RESET_ERRORS.has(code) || (code === "ECONNREFUSED" && alive))){
        if(!serialNodes.has(queueKey)){
            serialNodes.add(queueKey);
            SyncLog.log("warning", "device_http", "Device " + queueKey + " (" + origin + ") dropped a connection while handling several requests (" +
                (e.message || code) + ") — its requests go one at a time from now on.");
        }
        return code === "ECONNREFUSED" || method === "get" || !!opts.idempotent;
    }
    if(CONNECT_ERRORS.has(code)){
        originSkips.set(origin, { until: Date.now() + SKIP_MS, code, what: "did not take a connection", kind: "all" });
    }else if(code === "ECONNABORTED" && method === "get" && !opts.background && !originSkips.has(origin)){
        // A read went unanswered: skip further reads that have a fallback,
        // not the writes — a slow 5 s read says little about a 30 s PATCH.
        originSkips.set(origin, { until: Date.now() + SKIP_MS, code, what: "left a read unanswered", kind: "reads" });
    }
    return false;
}

/**
 * One request to a device: waits for its turn on the device, then runs on a
 * fresh connection. A request the device dropped while busy with others is
 * sent once more when that is safe. A background read that hits a skip
 * waits for the skip to end and tries once more.
 */
export function deviceRequest<T = any>(queueKey:string, config:AxiosRequestConfig, opts:DeviceRequestOptions = {}):Promise<AxiosResponse<T>>{
    return new Promise<AxiosResponse<T>>((resolve, reject) => {
        let resent = false;
        let deferred = false;
        const background = !!opts.background;
        const task:Task = async () => {
            let crowded = busy(queueKey) > 1;
            try{
                resolve(await attempt(queueKey, config, opts));
            }catch(e:any){
                crowded = crowded || busy(queueKey) > 1;
                if(learn(queueKey, config, opts, e, crowded) && !resent){
                    resent = true;
                    enqueue(queueKey, task, background, true);
                    return;
                }
                if(background && neverConnected(e) && !deferred){
                    // Nobody waits for it: try once more when the skip is over.
                    deferred = true;
                    const at = typeof e.retryAt === "number" ? e.retryAt :
                               (originSkips.get(originOf(config.url || ""))?.until || Date.now() + SKIP_MS);
                    setTimeout(() => enqueue(queueKey, task, background), Math.max(0, at - Date.now()) + 100);
                    return;
                }
                reject(e);
            }
        };
        enqueue(queueKey, task, background);
    });
}

/**
 * Several requests to a device as one unit: `body` holds one of the device's
 * slots until it returns, and nothing else of that device runs in between
 * once the device takes one request at a time. Inside, send with the
 * `send` handed in — deviceRequest() on the same queue would wait for the
 * slot `body` itself holds.
 */
export function withDevice<T>(queueKey:string, body:(send:DeviceSend) => Promise<T>):Promise<T>{
    return new Promise<T>((resolve, reject) => {
        const send:DeviceSend = async (config, opts = {}) => {
            let crowded = busy(queueKey) > 1;
            try{
                return await attempt(queueKey, config, opts);
            }catch(e:any){
                if(learn(queueKey, config, { ...opts, idempotent: false }, e, crowded || busy(queueKey) > 1) && e){
                    e.resend = true;
                }
                throw e;
            }
        };
        enqueue(queueKey, async () => {
            try{
                resolve(await body(send));
            }catch(e){
                reject(e);
            }
        }, false);
    });
}
