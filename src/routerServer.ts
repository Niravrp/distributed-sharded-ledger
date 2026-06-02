import { createServer } from 'node:http';
import { Kafka } from 'kafkajs';
import { ConsistentHashRouter } from './consistentHashRouter.js';

const PORT = process.env.PORT || 5000;
const router = new ConsistentHashRouter(100);

router.addNode('shard-eastus');
router.addNode('shard-westus');
router.addNode('shard-northeurope');

const kafka = new Kafka({
    clientId: 'api-gateway-proxy',
    brokers: [process.env.KAFKA_BROKER || 'kafka:9092']
});
const producer = kafka.producer();

async function startGateway() {
    let connected = false;
    while (!connected) {
        try {
            await producer.connect();
            connected = true;
            console.log("⚡ [GATEWAY PRODUCER] Connected to Apache Kafka broker cleanly.");
        } catch (err: any) {
            console.warn(`⚠️ [GATEWAY] Kafka broker not ready yet. Retrying in 2 seconds...`);
            await new Promise(resolve => setTimeout(resolve, 2000));
        }
    }

    const server = createServer(async (req, res) => {
        const urlObj = new URL(req.url || '', `http://${req.headers.host}`);

        // ─── SYNCHRONOUS READ PATH (BYPASSES KAFKA) ───
        if (req.method === 'GET' && urlObj.pathname === '/transaction') {
            const accountId = urlObj.searchParams.get('accountId');
            if (!accountId) {
                res.writeHead(400, { 'Content-Type': 'text/plain' });
                return res.end('Missing accountId parameter');
            }

            try {
                const targetNodes = router.getNodes(accountId, 3);
                // Query primary node first
                const primaryNode = targetNodes[0];
                const response = await fetch(`http://${primaryNode}:5001/read?key=${accountId}`);

                if (response.status === 200) {
                    const data = await response.json();
                    res.writeHead(200, { 'Content-Type': 'application/json' });
                    return res.end(JSON.stringify(data));
                }

                res.writeHead(404, { 'Content-Type': 'application/json' });
                return res.end(JSON.stringify({ error: "Account not found" }));
            } catch (err: any) {
                res.writeHead(500, { 'Content-Type': 'text/plain' });
                return res.end(`Read Routing Error: ${err.message}`);
            }
        }

        // ─── HYBRID RESERVATION WRITE PATH (STRONG AUTHORIZATION + ASYNC LEDGER) ───
        else if (req.method === 'POST' && urlObj.pathname === '/transaction') {
            let body = '';
            req.on('data', chunk => { body += chunk; });
            req.on('end', async () => {
                try {
                    const { accountId, amount, currency } = JSON.parse(body);
                    const transactionId = `tx_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`;

                    const targetNodes = router.getNodes(accountId, 3);
                    const primaryNode = targetNodes[0];

                    console.log(`\n🔒 [RESERVATION STEP] Locking authorization path on Primary Shard [${primaryNode}] for account [${accountId}]`);

                    // 1. Synchronously fetch current balance state
                    let currentBalance = 0;
                    try {
                        const readRes = await fetch(`http://${primaryNode}:5001/read?key=${accountId}`);
                        if (readRes.ok) {
                            const rawData = await readRes.json() as any;

                            // 🔍 DEBUG LINE: This will print the EXACT object structure to your terminal logs
                            console.log(`🔍 [GATEWAY DEBUG] Raw Shard Response Payload: ${JSON.stringify(rawData)}`);

                            // 2. POLYMORPHIC EXTRACTOR: Check every possible structural shape safely
                            if (rawData !== null && typeof rawData === 'object') {
                                if (typeof rawData.balance === 'number') {
                                    currentBalance = rawData.balance;
                                }
                                else if (rawData.record && typeof rawData.record.balance === 'number') {
                                    currentBalance = rawData.record.balance;
                                    console.log(`🎯 [GATEWAY BAL] Parsed balance from record envelope: $${currentBalance}`);
                                }
                                else if (rawData.payload && typeof rawData.payload.balance === 'number') {
                                    currentBalance = rawData.payload.balance;
                                }
                                else if (rawData.value && typeof rawData.value.balance === 'number') {
                                    currentBalance = rawData.value.balance;
                                }
                                else {
                                    console.warn(`⚠️ [GATEWAY BAL] Balance field not found in response objects. Defaulting to $0.`);
                                    currentBalance = 0;
                                }
                            }
                        }
                    } catch (readErr) {
                        console.log(`ℹ️ [GATEWAY] Account read failed or doesn't exist yet. Defaulting to $0.`);
                        currentBalance = 0;
                    }

                    // 3. Compute new balance
                    const newBalance = currentBalance + amount;

                    // 4. MATHEMATICAL GATEKEEPER: Stop NaN from ever moving forward
                    if (Number.isNaN(newBalance)) {
                        console.error(`❌ [CRITICAL FAULT] Balance calculation resulted in NaN! currentBalance: ${currentBalance}, amount: ${amount}`);
                        res.writeHead(500, { 'Content-Type': 'application/json' });
                        return res.end(JSON.stringify({ error: "Internal Mathematical Conversion Fault" }));
                    }

                    // 5. EVALUATE FUNDS
                    if (newBalance < 0) {
                        console.error(`❌ [RESERVATION REJECTED] Account [${accountId}] insufficient funds. Balance: ${currentBalance}, Request: ${amount}`);
                        res.writeHead(402, { 'Content-Type': 'application/json' });
                        return res.end(JSON.stringify({ error: "Insufficient Funds", currentBalance }));
                    }

                    console.log(`📝 [RESERVATION APPROVED] Deduced balance locally on [${primaryNode}]. New Balance: ${newBalance}`);

                    // 6. Synchronously commit the balance reservation to primary memory
                    const writeRes = await fetch(`http://${primaryNode}:5001/write`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            key: accountId,
                            payload: { balance: newBalance, currency, updatedAt: Date.now() }
                        })
                    });

                    if (!writeRes.ok) throw new Error("Primary Shard memory reservation write failed.");

                    // 7. Offload the paperwork to Kafka
                    const eventPayload = { transactionId, accountId, balance: newBalance, currency, timestamp: Date.now(), primaryNode };
                    await producer.send({
                        topic: 'ledger-transactions',
                        messages: [{ key: accountId, value: JSON.stringify(eventPayload) }],
                    });

                    res.writeHead(200, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ status: "Success", transactionId, balance: newBalance }));

                } catch (err: any) {
                    console.error(`❌ [GATEWAY ERROR] Transaction failure: ${err.message}`);
                    if (!res.headersSent) {
                        res.writeHead(500, { 'Content-Type': 'text/plain' });
                        res.end(`Transaction Processing Fault: ${err.message}`);
                    }
                }
            });
        } else {
            res.writeHead(404);
            res.end();
        }
    });

    server.listen(PORT, () => {
        console.log(`🚀 CQRS Strong-Reservation Gateway active on port ${PORT}`);
    });
}

startGateway().catch(console.error);