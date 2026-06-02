import * as fs from 'fs/promises';
import * as path from 'path';

// Define strict data contracts for our financial ledger payloads
export interface LedgerPayload {
    balance: number;
    currency: string;
    updatedAt: number;
}

interface LogEntry {
    op: 'SET';
    key: string;
    val: LedgerPayload;
}

export class MiniLSMEngine {
    private dataDir: string;
    private memtableMaxSize: number;
    private memtable: Map<string, LedgerPayload>;
    private walPath: string;

    constructor(dataDir: string = process.env.STORAGE_DIR || './lsm_data', memtableMaxSize: number = 3) {
        this.dataDir = dataDir;
        this.memtableMaxSize = memtableMaxSize;
        this.memtable = new Map<string, LedgerPayload>();
        this.walPath = path.join(dataDir, 'wal.log');
    }

    public async init(): Promise<void> {
        // Enforce physical file structures on local disk or mounted Azure file shares
        await fs.mkdir(this.dataDir, { recursive: true });
        await this._recoverFromWAL();
    }

    public async write(key: string, value: LedgerPayload): Promise<void> {
        // 1. Durability Step: Append entry to raw Write-Ahead Log string
        const logEntry: LogEntry = { op: 'SET', key, val: value };
        const serializedLog = JSON.stringify(logEntry) + '\n';
        await fs.appendFile(this.walPath, serializedLog, 'utf8');

        // 2. High-Throughput Step: Push directly into RAM Memtable
        this.memtable.set(key, value);
        console.log(`[MemTable RAM] Written: ${key} -> ${JSON.stringify(value)}`);

        // 3. Volatility Management: Flush to disk if RAM constraints are breached
        if (this.memtable.size >= this.memtableMaxSize) {
            await this._flushMemtable();
        }
    }

    public async read(key: string): Promise<LedgerPayload | null> {
        // 1. Memory Search: Check the fast RAM pool first
        if (this.memtable.has(key)) {
            console.log(`🔍 [LSM READ] Memory Hit! Found [${key}] inside RAM MemTable.`);
            return this.memtable.get(key)!;
        }

        // 2. Disk Search: Scan the SSTable files on disk
        console.log(`💾 [LSM READ] Memory Miss. Scanning physical SSTables on disk for [${key}]...`);
        try {
            const files = await fs.readdir(this.dataDir);
            
            // Isolate only the SSTable JSON files
            const sstableFiles = files.filter(f => f.startsWith('sstable_') && f.endsWith('.json'));
            
            if (sstableFiles.length === 0) return null;

            // Sort descending: Extract timestamps and sort newest first
            sstableFiles.sort((a, b) => {
                const timeA = parseInt(a.split('_')[1].split('.')[0]);
                const timeB = parseInt(b.split('_')[1].split('.')[0]);
                return timeB - timeA; // Newest timestamp comes first
            });

            // Look inside each SSTable chunk sequentially
            for (const file of sstableFiles) {
                const filePath = path.join(this.dataDir, file);
                const fileContent = await fs.readFile(filePath, 'utf8');
                const sstableData: Record<string, LedgerPayload> = JSON.parse(fileContent);

                if (sstableData[key]) {
                    console.log(`🎯 [LSM READ] Disk Hit! Isolated [${key}] inside older chunk: ${file}`);
                    return sstableData[key];
                }
            }
        } catch (err: any) {
            if (err.code !== 'ENOENT') throw err;
        }

        return null; // Key truly does not exist anywhere in the database
    }

    public async compact(): Promise<void> {
        console.log(`\n🧹 [COMPACTION] Background lifecycle worker triggered in: ${this.dataDir}`);
        try {
            const files = await fs.readdir(this.dataDir);
            const sstableFiles = files.filter(f => f.startsWith('sstable_') && f.endsWith('.json'));

            // Size-Tiered Threshold Check: Skip if there aren't enough fragments to justify disk I/O costs
            if (sstableFiles.length < 3) {
                console.log(`ℹ️ [COMPACTION] Only ${sstableFiles.length} SSTable chunks found. Skipping compaction until threshold (3) is hit.`);
                return;
            }

            console.log(`⚙️ [COMPACTION] Compacting ${sstableFiles.length} fragmented files...`);

            // Temporary map to collect deduplicated and freshest keys across files
            const mergedData: Record<string, LedgerPayload> = {};

            // Sort ascending: Oldest files first so newer updates overwrite older states naturally
            sstableFiles.sort((a, b) => {
                const timeA = parseInt(a.split('_')[1].split('.')[0]);
                const timeB = parseInt(b.split('_')[1].split('.')[0]);
                return timeA - timeB;
            });

            // Read every file sequentially and parse its records into the merge map
            for (const file of sstableFiles) {
                const filePath = path.join(this.dataDir, file);
                const fileContent = await fs.readFile(filePath, 'utf8');
                const sstableData: Record<string, LedgerPayload> = JSON.parse(fileContent);

                for (const [key, payload] of Object.entries(sstableData)) {
                    // Conflict Resolution Strategy: If data exists, choose the one with the highest updatedAt timestamp
                    if (!mergedData[key] || payload.updatedAt >= mergedData[key].updatedAt) {
                        mergedData[key] = payload;
                    }
                }
            }

            // Enforce strict alphabetical key layout before committing to disk
            const sortedKeys = Object.keys(mergedData).sort();
            const finalizedData: Record<string, LedgerPayload> = {};
            for (const key of sortedKeys) {
                finalizedData[key] = mergedData[key];
            }

            // Commit the fully consolidated data chunk back to disk
            const compactedTimestamp = Date.now();
            const compactedFileName = `sstable_${compactedTimestamp}.json`;
            const compactedFilePath = path.join(this.dataDir, compactedFileName);

            await fs.writeFile(compactedFilePath, JSON.stringify(finalizedData, null, 2), 'utf8');
            console.log(`✨ [COMPACTION] Successfully consolidated data into clean master chunk: ${compactedFileName}`);

            // Garbage Collection Step: Atomically delete the old, redundant SSTable chunks
            for (const file of sstableFiles) {
                const oldFilePath = path.join(this.dataDir, file);
                await fs.unlink(oldFilePath);
                console.log(`🗑️ [COMPACTION] Dropped redundant file from system storage: ${file}`);
            }
            console.log(`✅ [COMPACTION COMPLETE] Disk cleanup finalized.\n`);

        } catch (err: any) {
            console.error(`❌ [COMPACTION ERROR] Catastrophic failure during structural merge: ${err.message}`);
        }
    }

    private async _flushMemtable(): Promise<void> {
        const timestamp = Date.now();
        const sstablePath = path.join(this.dataDir, `sstable_${timestamp}.json`);

        // Force alphabetic serialization to maintain strict SSTable string order
        const sortedKeys = Array.from(this.memtable.keys()).sort();
        const sortedData: Record<string, LedgerPayload> = {};
        for (const key of sortedKeys) {
            sortedData[key] = this.memtable.get(key)!;
        }

        await fs.writeFile(sstablePath, JSON.stringify(sortedData, null, 2), 'utf8');
        console.log(`\n⚡ [FLUSH] MemTable saturated. Created immutable SSTable chunk: ${sstablePath}`);

        // Purge memory allocations and drain WAL cleanly
        this.memtable.clear();
        await fs.writeFile(this.walPath, '', 'utf8');
        console.log("🧹 [CLEAN] RAM pools purged. WAL reference zeroed.");

        // Automatically trigger compaction pipeline to evaluate disk health asynchronously
        await this.compact();
    }

    private async _recoverFromWAL(): Promise<void> {
        try {
            const data = await fs.readFile(this.walPath, 'utf8');
            if (!data.trim()) return;

            console.log("🔄 [BOOT] Active WAL log detected. Rebuilding state cache...");
            const lines = data.split('\n');
            
            for (const line of lines) {
                if (line.trim()) {
                    const entry: LogEntry = JSON.parse(line);
                    if (entry.op === 'SET') {
                        this.memtable.set(entry.key, entry.val);
                    }
                }
            }
            console.log(`✅ [RECOVERY] MemTable reconstructed in RAM:`, Object.fromEntries(this.memtable), `\n`);
        } catch (err: any) {
            if (err.code !== 'ENOENT') throw err;
        }
    }
}