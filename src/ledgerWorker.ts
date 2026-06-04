import { Kafka } from 'kafkajs';
import { ConsistentHashRouter } from './consistentHashRouter.js';

// 🌍 CLOUD URL RESOLVER: Maps local names to global Azure endpoints
function getShardBaseUrl(nodeName: string): string {
    const cloudMap: Record<string, string | undefined> = {
        'shard-eastus': process.env.SHARD_EASTUS_URL,
        'shard-westus': process.env.SHARD_WESTUS_URL,
        'shard-northeurope': process.env.SHARD_NORTHEUROPE_URL,
    };
    
    const cloudUrl = cloudMap[nodeName];
    return cloudUrl ? cloudUrl : `http://${nodeName}:5001`;
}

const router = new ConsistentHashRouter(100);
router.addNode('shard-eastus');
router.addNode('shard-westus');
router.addNode('shard-northeurope');

const kafkaBroker = process.env.KAFKA_BROKER || 'kafka:9092';
const connectionString = process.env.KAFKA_CONNECTION_STRING;

const kafkaConfig: any = {
    clientId: 'ledger-worker-service',
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
    console.log("🔒 [WORKER INITIALIZATION] SASL_SSL encryption profile applied.");
}

const kafka = new Kafka(kafkaConfig);
const consumer = kafka.consumer({ groupId: 'ledger-workers-cluster' });

async function startWorker() {
    console.log("⚙️ Starting Resilient Distributed Ledger Event Consumer Engine...");
    
    let initialized = false;
    let connected = false;

    while (!initialized) {
        try {
            if (!connected) {
                await consumer.connect();
                connected = true;
                console.log("⚡ [WORKER CONSUMER] Connected to Apache Kafka broker cleanly.");
            }
            await consumer.subscribe({ topic: 'ledger-transactions', fromBeginning: true });
            initialized = true; 
            console.log("📥 [WORKER BUS] Successfully subscribed to topic [ledger-transactions]. Processing pipeline active.");
        } catch (err: any) {
            console.warn(`⚠️ [WORKER INIT WARN] Setup stalled: ${err.message}. Retrying in 3 seconds...`);
            await new Promise(resolve => setTimeout(resolve, 3000));
        }
    }

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            if (!message.value) return;
            
            try {
                const event = JSON.parse(message.value.toString());
                const { accountId, balance, currency, timestamp, primaryNode } = event;

                // Find all target nodes on the ring
                const allNodes = router.getNodes(accountId, 3);
                
                // CRITICAL SEPARATION: Filter out the primaryNode because it was already updated synchronously!
                const backupNodes = allNodes.filter(node => node !== primaryNode);

                console.log(`\n🧵 [WORKER ORECHESTRATION] Processing transaction ${event.transactionId}. Primary was [${primaryNode}]. Asynchronously replicating to backups: ${JSON.stringify(backupNodes)}`);

                const payload = { balance, currency, updatedAt: timestamp };

                // Replicate exclusively to the backup nodes across regions
                const replicationPromises = backupNodes.map(async (node) => {
                    const baseUrl = getShardBaseUrl(node); // 🌍 Resolve to cloud URL
                    console.log(`[WORKER] Replicating to ${baseUrl}...`);

                    const response = await fetch(`${baseUrl}/write`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ key: accountId, payload })
                    });
                    if (!response.ok) throw new Error(`Backup Shard Node [${node}] rejected write execution.`);
                    return response.json();
                });

                await Promise.all(replicationPromises);
                console.log(`✅ [WORKER EFFECT] Asynchronous multi-region replication verified for trace ID: ${event.transactionId}`);

            } catch (err: any) {
                console.error(`❌ [WORKER PROCESSING ERROR] Failure on message offset processing: ${err.message}`);
            }
        },
    });
}

startWorker().catch(console.error);