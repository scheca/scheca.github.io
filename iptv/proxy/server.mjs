// Runs proxy/worker.js on plain Node (>= 18), e.g. on a Raspberry Pi, serving HTTPS.
//
//   TLS_CERT=/etc/iptv-proxy/fullchain.pem TLS_KEY=/etc/iptv-proxy/privkey.pem \
//   ALLOWED_ORIGINS=https://scheca.github.io ALLOWED_HOSTS=proveedor.com:8080 \
//   node server.mjs
//
// Environment:
//   PORT             listening port (default 8443)
//   HOST             listening address (default 0.0.0.0)
//   TLS_CERT/TLS_KEY PEM files; without them it serves plain HTTP (only for testing: the app is
//                    HTTPS, so the browser will refuse an http:// proxy)
//   ALLOWED_ORIGINS, ALLOWED_HOSTS   same as in worker.js

import http from 'node:http';
import https from 'node:https';
import fs from 'node:fs';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import worker from './worker.js';

const port = Number(process.env.PORT) || 8443;
const host = process.env.HOST || '0.0.0.0';
const { TLS_CERT, TLS_KEY } = process.env;
const env = { ALLOWED_ORIGINS: process.env.ALLOWED_ORIGINS, ALLOWED_HOSTS: process.env.ALLOWED_HOSTS };

async function handle(req, res, scheme) {
  const headers = new Headers();
  for (const [k, v] of Object.entries(req.headers)) {
    if (Array.isArray(v)) v.forEach(x => headers.append(k, x)); else if (v != null) headers.set(k, v);
  }
  // The worker builds the URLs it writes into playlists from request.url: use the public address.
  const url = `${scheme}://${req.headers.host || 'localhost'}${req.url}`;
  const hasBody = !['GET', 'HEAD'].includes(req.method);
  const request = new Request(url, {
    method: req.method,
    headers,
    body: hasBody ? Readable.toWeb(req) : undefined,
    duplex: hasBody ? 'half' : undefined,
  });

  const response = await worker.fetch(request, env);
  res.writeHead(response.status, Object.fromEntries(response.headers));
  if (!response.body || req.method === 'HEAD') return res.end();
  // pipeline() tears the upstream download down as soon as the player disconnects, so a live
  // channel does not keep eating the provider's (often single) connection.
  await pipeline(Readable.fromWeb(response.body), res);
}

function listener(scheme) {
  return (req, res) => {
    handle(req, res, scheme).catch(e => {
      if (e?.code === 'ERR_STREAM_PREMATURE_CLOSE' || e?.name === 'AbortError') return;   // player went away
      console.error(new Date().toISOString(), req.method, req.url, e?.message || e);
      if (!res.headersSent) res.writeHead(502, { 'Content-Type': 'text/plain' });
      res.end('Proxy error');
    });
  };
}

let server;
if (TLS_CERT && TLS_KEY) {
  const tls = () => ({ cert: fs.readFileSync(TLS_CERT), key: fs.readFileSync(TLS_KEY) });
  server = https.createServer(tls(), listener('https'));
  // Pick up a renewed certificate without restarting (Let's Encrypt renews every ~60 days).
  fs.watchFile(TLS_CERT, { interval: 3600e3 }, () => {
    try { server.setSecureContext(tls()); console.log('Certificate reloaded'); } catch (e) { console.error('Certificate reload failed:', e.message); }
  });
} else {
  console.warn('TLS_CERT/TLS_KEY not set: serving plain HTTP (testing only)');
  server = http.createServer(listener('http'));
}
server.requestTimeout = 0;          // streams last hours
server.keepAliveTimeout = 65e3;
server.listen(port, host, () => console.log(`IPTV proxy listening on ${TLS_CERT ? 'https' : 'http'}://${host}:${port}`));
