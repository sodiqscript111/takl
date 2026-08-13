async function fetchNodes() {
    const res = await fetch('/api/nodes');
    return await res.json();
}

async function fetchClusterState(nodeId) {
    try {
        const res = await fetch(`/api/nodes/${nodeId}/proxy/api/v1/cluster`);
        if (!res.ok) return null;
        return await res.json();
    } catch {
        return null;
    }
}

async function actionNode(nodeId, action) {
    await fetch(`/api/nodes/${nodeId}/${action}`, { method: 'POST' });
    updateUI();
}

async function startAll() {
    const nodes = await fetchNodes();
    for (const n of nodes) {
        if (!n.running) {
            await fetch(`/api/nodes/${n.id}/start`, { method: 'POST' });
        }
    }
    updateUI();
}

async function killAll() {
    const nodes = await fetchNodes();
    for (const n of nodes) {
        if (n.running) {
            await fetch(`/api/nodes/${n.id}/stop`, { method: 'POST' });
        }
    }
    updateUI();
}

async function runChaos() {
    await startAll();
    let duration = 120;
    const interval = setInterval(async () => {
        duration -= 5;
        if (duration <= 0) {
            clearInterval(interval);
            return;
        }
        const nodes = await fetchNodes();
        const n = nodes[Math.floor(Math.random() * nodes.length)];
        if (n.running) {
            await fetch(`/api/nodes/${n.id}/stop`, { method: 'POST' });
        } else {
            await fetch(`/api/nodes/${n.id}/start`, { method: 'POST' });
        }
        updateUI();
    }, 5000);
}

document.getElementById('start-all').onclick = startAll;
document.getElementById('kill-all').onclick = killAll;
document.getElementById('run-chaos').onclick = runChaos;

function renderNode(node, state) {
    const isOffline = !node.running;
    const s = state || {};
    return `
        <div class="card ${isOffline ? 'offline' : ''}">
            <div class="card-header">
                <div class="node-id">
                    <div class="status-dot"></div>
                    ${node.id.toUpperCase()}
                </div>
            </div>
            
            <div class="metric">
                <span class="metric-label">Cluster View (Active Runners)</span>
                <span class="metric-value">${s.active_runners !== undefined ? s.active_runners : '-'} / 4</span>
            </div>
            <div class="metric">
                <span class="metric-label">Global Active Builds</span>
                <span class="metric-value">${s.active_builds !== undefined ? s.active_builds : '-'}</span>
            </div>
            <div class="metric">
                <span class="metric-label">Cluster Avg CPU</span>
                <span class="metric-value">${s.avg_cpu_util !== undefined ? (s.avg_cpu_util * 100).toFixed(1) + '%' : '-'}</span>
            </div>
            
            <div class="controls">
                <button class="btn primary" ${node.running ? 'disabled' : ''} onclick="actionNode('${node.id}', 'start')">Boot</button>
                <button class="btn danger" ${!node.running ? 'disabled' : ''} onclick="actionNode('${node.id}', 'stop')">Kill</button>
            </div>
        </div>
    `;
}

async function updateUI() {
    const nodes = await fetchNodes();
    const grid = document.getElementById('nodes-grid');
    
    let html = '';
    for (const node of nodes) {
        let state = null;
        if (node.running) {
            state = await fetchClusterState(node.id);
        }
        html += renderNode(node, state);
    }
    grid.innerHTML = html;
}

updateUI();
setInterval(updateUI, 2000);
