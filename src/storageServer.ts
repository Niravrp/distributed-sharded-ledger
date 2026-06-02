import { createServer } from 'node:http';
import { MiniLSMEngine } from './lsmEngine.js';

const PORT = process.env.PORT || 5001;
const NODE_NAME = process.env.NODE_NAME || 'unknown-node';
const STORAGE_DIR = process.env.STORAGE_DIR || `./cluster_storage/${NODE_NAME}`;

const engine = new MiniLSMEngine(STORAGE_DIR, 3);

async function start() {
    await engine.init();
    
    const server = createServer(async (req, res) => {
        const urlObj = new URL(req.url || '', `http://${req.headers.host}`);

        // --- READ PATH ---
        if (req.method === 'GET' && urlObj.pathname === '/read') {
            const key = urlObj.searchParams.get('key');
            if (!key) {
                res.writeHead(400, { 'Content-Type': 'text/plain' });
                return res.end('Missing key query parameter');
            }

            try {
                const record = await engine.read(key);
                if (!record) {
                    res.writeHead(404, { 'Content-Type': 'application/json' });
                    return res.end(JSON.stringify({ error: 'Account Not Found' }));
                }
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ node: NODE_NAME, record }));
                return; // Explicitly halt execution path
            } catch (err: any) {
                console.error(`❌ [STORAGE ERROR] Exception in read execution: ${err.message}`);
                if (res.headersSent) return; // Guardrail against double-headers
                res.writeHead(500, { 'Content-Type': 'text/plain' });
                return res.end(`Engine Error: ${err.message}`);
            }
        } 
        // --- WRITE PATH ---
        else if (req.method === 'POST' && urlObj.pathname === '/write') {
            let body = '';
            req.on('data', chunk => { body += chunk; });
            req.on('end', async () => {
                try {
                    const { key, payload } = JSON.parse(body);
                    await engine.write(key, payload);
                    res.writeHead(200, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ status: 'success', node: NODE_NAME }));
                    return;
                } catch (err: any) {
                    console.error(`❌ [STORAGE ERROR] Exception in write execution: ${err.message}`);
                    if (res.headersSent) return;
                    res.writeHead(400, { 'Content-Type': 'text/plain' });
                    return res.end(`Write Error: ${err.message}`);
                }
            });
        } else {
            res.writeHead(404);
            return res.end();
        }
    });

    server.listen(PORT, () => {
        console.log(`📡 Storage Node [${NODE_NAME}] online inside container network.`);
    });
}

start().catch(console.error);