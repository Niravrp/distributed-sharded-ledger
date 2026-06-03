import { createServer } from 'node:http';
import { Kafka } from 'kafkajs';
import { ConsistentHashRouter } from './consistentHashRouter.js';

const PORT = process.env.PORT || 5000;
const router = new ConsistentHashRouter(100);

router.addNode('shard-eastus');
router.addNode('shard-westus');
router.addNode('shard-northeurope');

// 🌍 CLOUD URL RESOLVER: Maps local names to global Azure endpoints
function getShardBaseUrl(nodeName: string): string {
    const cloudMap: Record<string, string | undefined> = {
        'shard-eastus': process.env.SHARD_EASTUS_URL,
        'shard-westus': process.env.SHARD_WESTUS_URL,
        'shard-northeurope': process.env.SHARD_NORTHEUROPE_URL,
    };
    
    const cloudUrl = cloudMap[nodeName];
    // If we are in Azure, use the secure cloud URL (drops the :5001). Otherwise, fallback to local Docker.
    return cloudUrl ? cloudUrl : `http://${nodeName}:5001`;
}

// 🔐 CLOUD KAFKA CONFIGURATION DETECTION
const kafkaBroker = process.env.KAFKA_BROKER || 'kafka:9092';
const connectionString = process.env.KAFKA_CONNECTION_STRING;

const kafkaConfig: any = {
    clientId: 'api-gateway-proxy',
    brokers: [kafkaBroker],
    connectionTimeout: 10000,     
    authenticationTimeout: 10000,
};

if (connectionString) {
    kafkaConfig.ssl = true;
    kafkaConfig.sasl = {
        mechanism: 'plain',
        username: '$ConnectionString', 
        password: connectionString     
    };
}

const kafka = new Kafka(kafkaConfig);
const producer = kafka.producer();

async function startGateway() {
    let connected = false;
    while (!connected) {
        try {
            await producer.connect();
            connected = true;
            console.log("⚡ [GATEWAY PRODUCER] Connected to Apache Kafka broker cleanly.");
        } catch (err: any) {
            console.warn(`⚠️ [GATEWAY] Kafka connection failed: ${err.message}. Retrying in 2 seconds...`);
            await new Promise(resolve => setTimeout(resolve, 2000));
        }
    }

    const server = createServer(async (req, res) => {
        const urlObj = new URL(req.url || '', `http://${req.headers.host}`);

        // ─── SYNCHRONOUS READ PATH ───
        if (req.method === 'GET' && urlObj.pathname === '/transaction') {
            const accountId = urlObj.searchParams.get('accountId');
            if (!accountId) {
                res.writeHead(400, { 'Content-Type': 'text/plain' });
                return res.end('Missing accountId parameter');
            }

            try {
                const targetNodes = router.getNodes(accountId, 3);
                const primaryNode = targetNodes[0];
                const baseUrl = getShardBaseUrl(primaryNode); // 🌍 Resolved URL

                const response = await fetch(`${baseUrl}/read?key=${accountId}`);

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

        // ─── HYBRID RESERVATION WRITE PATH ───
        else if (req.method === 'POST' && urlObj.pathname === '/transaction') {
            let body = '';
            req.on('data', chunk => { body += chunk; });
            req.on('end', async () => {
                try {
                    const { accountId, amount, currency } = JSON.parse(body);
                    const transactionId = `tx_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`;

                    const targetNodes = router.getNodes(accountId, 3);
                    const primaryNode = targetNodes[0];
                    const baseUrl = getShardBaseUrl(primaryNode); // 🌍 Resolved URL

                    console.log(`\n🔒 [RESERVATION STEP] Locking authorization path on Primary Shard [${primaryNode}] for account [${accountId}]`);
                    console.log(`📡 [NETWORK] Routing cross-ocean HTTP request to: ${baseUrl}`);

                    let currentBalance = 0;
                    try {
                        const readRes = await fetch(`${baseUrl}/read?key=${accountId}`);
                        if (readRes.ok) {
                            const rawData = await readRes.json() as any;
                            if (rawData !== null && typeof rawData === 'object') {
                                if (typeof rawData.balance === 'number') currentBalance = rawData.balance;
                                else if (rawData.record && typeof rawData.record.balance === 'number') currentBalance = rawData.record.balance;
                                else if (rawData.payload && typeof rawData.payload.balance === 'number') currentBalance = rawData.payload.balance;
                                else if (rawData.value && typeof rawData.value.balance === 'number') currentBalance = rawData.value.balance;
                            }
                        }
                    } catch (readErr) {
                        console.log(`ℹ️ [GATEWAY] Account read failed or doesn't exist yet. Defaulting to $0.`);
                    }

                    const newBalance = currentBalance + amount;

                    if (Number.isNaN(newBalance)) {
                        res.writeHead(500, { 'Content-Type': 'application/json' });
                        return res.end(JSON.stringify({ error: "Internal Mathematical Conversion Fault" }));
                    }

                    if (newBalance < 0) {
                        res.writeHead(402, { 'Content-Type': 'application/json' });
                        return res.end(JSON.stringify({ error: "Insufficient Funds", currentBalance }));
                    }

                    console.log(`📝 [RESERVATION APPROVED] Deduced balance locally on [${primaryNode}]. New Balance: ${newBalance}`);

                    const writeRes = await fetch(`${baseUrl}/write`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            key: accountId,
                            payload: { balance: newBalance, currency, updatedAt: Date.now() }
                        })
                    });

                    if (!writeRes.ok) throw new Error("Primary Shard memory reservation write failed.");

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