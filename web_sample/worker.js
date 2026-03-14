// Web Worker for iris-blocks WASM module
// Runs the BlockExporter in a dedicated worker thread, communicating stats and
// query results back to the main thread via postMessage.

import init, { BlockExporter, setLogging } from './pkg/iris_blocks.js';

let exporter = null;

self.onmessage = async (e) => {
    const { type, ...data } = e.data;
    switch (type) {
        case 'init':
            await handleInit(data);
            break;
        case 'query':
            await handleQuery(data);
            break;
        case 'export':
            await handleExport();
            break;
        case 'stop':
            if (exporter) {
                await exporter.stop();
                exporter = null;
            }
            self.postMessage({ type: 'stopped' });
            break;
        case 'updateRpc':
            if (exporter) {
                try {
                    exporter.updateRpc(data.url || null);
                    self.postMessage({ type: 'status', message: data.url ? `RPC endpoint updated to ${data.url}` : 'RPC disconnected' });
                } catch (err) {
                    self.postMessage({ type: 'error', message: `Failed to update RPC: ${err}` });
                }
            }
            break;
    }
};

async function handleInit(data) {
    try {
        console.log('[worker] initializing wasm...');
        await init();
        self.postMessage({ type: 'status', message: 'WASM Initialized.' });

        if (data.logging) {
            setLogging(data.logging);
        }

        self.postMessage({ type: 'status', message: 'Creating BlockExporter...' });
        console.log('[worker] creating BlockExporter with config:', data.config);

        exporter = await new BlockExporter(data.config, data.dbBytes || undefined);

        console.log('[worker] BlockExporter created successfully');
        self.postMessage({ type: 'status', message: 'BlockExporter instance created successfully!' });
        self.postMessage({ type: 'ready' });

        // Start stats polling loops (non-blocking, fire-and-forget)
        pollStats('l0', () => exporter.nextL0Stats());
        pollStats('l1', () => exporter.nextL1Stats());
        pollStats('l2', () => exporter.nextL2Stats());
        pollStats('l3', () => exporter.nextL3Stats());
        pollStats('l4', () => exporter.nextL4Stats());
    } catch (err) {
        console.error('[worker] init error:', err);
        self.postMessage({ type: 'error', message: err.message || String(err) });
    }
}

async function pollStats(layer, fetchFn) {
    while (exporter) {
        try {
            const stats = await fetchFn();
            if (stats) {
                self.postMessage({ type: 'stats', layer, stats });
            }
        } catch (e) {
            // Exporter may have been stopped
            break;
        }
        await new Promise(r => setTimeout(r, 100));
    }
}

async function handleExport() {
    if (!exporter) {
        self.postMessage({ type: 'exportResult', error: 'Exporter not initialized' });
        return;
    }
    try {
        const bytes = await exporter.exportDb();
        const buffer = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
        self.postMessage({ type: 'exportResult', data: buffer }, [buffer]);
    } catch (err) {
        self.postMessage({ type: 'exportResult', error: String(err) });
    }
}

async function handleQuery({ id, sql }) {
    if (!exporter) {
        self.postMessage({ type: 'queryResult', id, error: 'Exporter not initialized' });
        return;
    }
    try {
        const result = await exporter.query(sql);
        self.postMessage({ type: 'queryResult', id, result });
    } catch (err) {
        self.postMessage({ type: 'queryResult', id, error: String(err) });
    }
}
