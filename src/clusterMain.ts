import { MiniLSMEngine, LedgerPayload } from './lsmEngine.js';
import { ConsistentHashRouter } from './consistentHashRouter.js';
import * as path from 'node:path';

async function runCrashSimulation() {
    console.log("🚀 Initializing Fault-Tolerant Cluster Mesh via TypeScript...");

    // 1. Setup the balanced ring with 100 virtual nodes
    const router = new ConsistentHashRouter(100);
    const nodes = ["azure-shard-eastus", "azure-shard-westus", "azure-shard-northeurope"];
    const clusterEngines: Record<string, MiniLSMEngine> = {};

    for (const node of nodes) {
        router.addNode(node);
        const storagePath = path.join('./cluster_storage', node);
        clusterEngines[node] = new MiniLSMEngine(storagePath, 3);
        await clusterEngines[node].init();
    }

    // 2. Stream Batch 1 (Normal Operations)
    const batch1 = [
        { accountId: "acc_4821", balance: 500 },
        { accountId: "acc_1092", balance: 2500 },
        { accountId: "acc_7734", balance: 90 }
    ];

    console.log("\n--- [BATCH 1] Streaming under healthy conditions ---");
    for (const tx of batch1) {
        const designatedNode = router.getNode(tx.accountId)!;
        console.log(`Routing [${tx.accountId}] ➔ [${designatedNode}]`);
        await clusterEngines[designatedNode].write(tx.accountId, { balance: tx.balance, currency: "USD", updatedAt: Date.now() });
    }

    // 3. 🚨 CRITICAL FAULT: Simulate azure-shard-westus catching fire and going offline!
    console.log("\n💥 💥 💥 CRITICAL ALARM: [azure-shard-westus] has crashed! Removing from ring...");
    router.removeNode("azure-shard-westus"); 

    // 4. Stream Batch 2 (Testing Cluster Self-Healing)
    const batch2 = [
        { accountId: "acc_3301", balance: 1400 },
        { accountId: "acc_8892", balance: 310 },
        { accountId: "acc_1102", balance: 75 }
    ];

    console.log("\n--- [BATCH 2] Streaming while [westus] is dead ---");
    for (const tx of batch2) {
        const designatedNode = router.getNode(tx.accountId)!;
        console.log(`Routing [${tx.accountId}] ➔ [${designatedNode}] (Self-Healed!)`);
        await clusterEngines[designatedNode].write(tx.accountId, { balance: tx.balance, currency: "USD", updatedAt: Date.now() });
    }
}

runCrashSimulation().catch(console.error);