import * as crypto from 'crypto';

export class ConsistentHashRouter {
    private vnodesCount: number;
    private ring: Map<string, string>; // Stores: Hash String -> Physical Node ID Mapping
    private sortedHashes: string[];

    constructor(vnodesCount: number = 3) {
        this.vnodesCount = vnodesCount;
        this.ring = new Map<string, string>();
        this.sortedHashes = [];
    }

    private _hash(key: string): string {
        return crypto.createHash('md5').update(key).digest('hex');
    }

    public addNode(node: string): void {
        for (let i = 0; i < this.vnodesCount; i++) {
            const vnodeKey = `${node}-vnode-${i}`;
            const hash = this._hash(vnodeKey);
            this.ring.set(hash, node);
            this.sortedHashes.push(hash);
        }
        this.sortedHashes.sort(); // Enforce ascending sorted array boundary for fast traversal
    }

    public removeNode(node: string): void {
        for (let i = 0; i < this.vnodesCount; i++) {
            const vnodeKey = `${node}-vnode-${i}`;
            const hash = this._hash(vnodeKey);
            this.ring.delete(hash); // Wipe the hash coordinate from the memory map
        }
        // Filter out the dead hashes from our sorted clockwise array
        this.sortedHashes = this.sortedHashes.filter(h => this.ring.has(h));
        console.log(`🧹 [ROUTER RING] Successfully wiped all virtual nodes for: ${node}`);
    }

    public getNode(key: string): string | null {
        if (this.sortedHashes.length === 0) return null;
        const hash = this._hash(key);

        // Scan ring boundary clockwise to isolate closest node segment
        for (const nodeHash of this.sortedHashes) {
            if (hash <= nodeHash) {
                return this.ring.get(nodeHash) || null;
            }
        }
        // Loop boundary wrapping fall-through
        return this.ring.get(this.sortedHashes[0]) || null;
    }

    public getNodes(key: string, replicationFactor: number = 2): string[] {
        if (this.sortedHashes.length === 0) return [];
        
        const hash = this._hash(key);
        const uniqueNodes = new Set<string>();
        
        // Find the starting index on the sorted ring array clockwise
        let startIndex = this.sortedHashes.findIndex(nodeHash => hash <= nodeHash);
        if (startIndex === -1) startIndex = 0; // Wrap around fallback

        // Walk clockwise around the ring until we gather enough unique physical shards
        for (let i = 0; i < this.sortedHashes.length; i++) {
            const currentIndex = (startIndex + i) % this.sortedHashes.length;
            const currentHash = this.sortedHashes[currentIndex];
            const physicalNode = this.ring.get(currentHash)!;

            uniqueNodes.add(physicalNode);

            // Halt once we've collected enough unique backup destinations
            if (uniqueNodes.size === replicationFactor) {
                break;
            }
        }

        return Array.from(uniqueNodes);
    }
}