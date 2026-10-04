// Bank Transaction Streamer Client Logic

const API_BASE = window.location.origin;

document.addEventListener('DOMContentLoaded', () => {
    loadTransactions();
    loadAnalytics();
    loadSystemStatus();

    // Poll for real-time updates every 3 seconds
    setInterval(() => {
        loadTransactions(false);
        loadAnalytics();
        loadSystemStatus();
    }, 3000);

    document.getElementById('paymentForm').addEventListener('submit', handlePaymentSubmit);

    // Initialize quick amount chip listeners
    document.querySelectorAll('.btn-chip').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.btn-chip').forEach(c => c.classList.remove('active'));
            btn.classList.add('active');
        });
    });
});

function setAmount(val) {
    document.getElementById('amount').value = val;
}

async function handlePaymentSubmit(e) {
    e.preventDefault();

    const senderId = document.getElementById('senderId').value.trim();
    const receiverId = document.getElementById('receiverId').value.trim();
    const amount = parseFloat(document.getElementById('amount').value);
    const transactionType = document.getElementById('transactionType').value;

    const btn = document.getElementById('btnSend');
    btn.disabled = true;
    btn.innerHTML = '<span class="material-symbols-outlined btn-icon">sync</span><span>Publishing to Kafka...</span>';

    // Highlight flow steps
    animatePipeline();

    try {
        const response = await fetch(`${API_BASE}/api/transactions`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ senderId, receiverId, amount, transactionType })
        });

        const result = await response.json();

        if (result.success) {
            showAlert(`Payment successfully published to Kafka topic 'bank-transactions'. Txn ID: ${result.transaction.transactionId}`, 'success');
            renderJsonPreview(result.transaction);

            // Reload table and stats
            setTimeout(() => {
                loadTransactions();
                loadAnalytics();
            }, 500);
        } else {
            showAlert(result.error || 'Failed to stream transaction.', 'error');
        }
    } catch (err) {
        showAlert(`Network/Server error: ${err.message}`, 'error');
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<span class="material-symbols-outlined btn-icon">send</span><span>SEND PAYMENT</span>';
    }
}

async function loadTransactions(showLoading = true) {
    const tbody = document.getElementById('transactionTableBody');
    const typeFilter = document.getElementById('typeFilter').value;
    const query = typeFilter ? `?type=${typeFilter}` : '';

    try {
        const res = await fetch(`${API_BASE}/api/transactions${query}`);
        const data = await res.json();

        if (!Array.isArray(data) || data.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" class="empty-state">No transactions recorded yet in MongoDB.</td></tr>';
            return;
        }

        tbody.innerHTML = data.map(txn => {
            const typeKey = (txn.transactionType || '').toLowerCase();
            return `
            <tr>
                <td><code>${txn.transactionId}</code></td>
                <td>${renderUserBadge(txn.senderId)}</td>
                <td>${renderUserBadge(txn.receiverId)}</td>
                <td><strong>₹${parseFloat(txn.amount).toLocaleString('en-IN', { minimumFractionDigits: 2 })}</strong></td>
                <td><span class="badge badge-${typeKey}">${txn.transactionType}</span></td>
                <td><span class="badge badge-success"><span class="material-symbols-outlined" style="font-size:13px;">check_circle</span> ${txn.status}</span></td>
                <td><small style="color: var(--text-muted);">${formatTime(txn.timestamp)}</small></td>
            </tr>
            `;
        }).join('');
    } catch (err) {
        if (showLoading) {
            tbody.innerHTML = `<tr><td colspan="7" class="empty-state error">Failed to load transactions: ${err.message}</td></tr>`;
        }
    }
}

async function loadAnalytics() {
    try {
        const res = await fetch(`${API_BASE}/api/analytics`);
        const stats = await res.json();

        document.getElementById('statTotalTxn').textContent = stats.totalTransactions || 0;
        document.getElementById('statTotalAmount').textContent = '₹' + (stats.totalAmount || 0).toLocaleString('en-IN', { minimumFractionDigits: 2 });
        document.getElementById('statAvgAmount').textContent = '₹' + (stats.averageAmount || 0).toLocaleString('en-IN', { minimumFractionDigits: 2 });
        document.getElementById('statUpi').textContent = stats.upiTransactions || 0;
        document.getElementById('statDebit').textContent = stats.debitTransactions || 0;
        document.getElementById('statCredit').textContent = stats.creditTransactions || 0;
    } catch (err) {
        console.warn('Analytics fetch error:', err);
    }
}

async function loadSystemStatus() {
    try {
        const res = await fetch(`${API_BASE}/api/status`);
        const status = await res.json();

        const mongoBadge = document.getElementById('mongoStatus');
        const mongoText = document.getElementById('mongoText');
        if (status.mongoConnected) {
            mongoBadge.className = 'status-badge';
            mongoBadge.innerHTML = `<span class="live-pulse"></span> <span class="material-symbols-outlined status-icon">database</span> <span>Mongo: <strong>Connected</strong></span>`;
        } else {
            mongoBadge.className = 'status-badge error';
            mongoBadge.innerHTML = `<span class="material-symbols-outlined status-icon" style="color: var(--danger);">error</span> <span>Mongo: <strong>Disconnected</strong></span>`;
        }

        const kafkaBadge = document.getElementById('kafkaStatus');
        kafkaBadge.innerHTML = `<span class="live-pulse"></span> <span class="material-symbols-outlined status-icon">hub</span> <span>Kafka: <strong>9092</strong></span>`;

        document.getElementById('consumerCount').textContent = `${status.consumerProcessedCount || 0} msgs`;
    } catch (err) {
        console.warn('Status fetch error:', err);
    }
}

function renderUserBadge(id) {
    if (!id) return '-';
    const initial = id.substring(0, 1).toUpperCase();
    return `<span class="user-badge"><span class="user-avatar">${initial}</span> ${id}</span>`;
}

function handleSearch() {
    const filter = document.getElementById('searchFilter').value.toLowerCase();
    const rows = document.querySelectorAll('#transactionTableBody tr');
    rows.forEach(r => {
        const text = r.textContent.toLowerCase();
        r.style.display = text.includes(filter) ? '' : 'none';
    });
}

async function generateSimulatedBatch(count) {
    const users = ['USER101', 'USER205', 'USER309', 'USER412', 'USER518', 'MERCHANT99'];
    const types = ['UPI_PAYMENT', 'IMPS', 'NEFT', 'DEBIT', 'CREDIT'];
    const amounts = [150, 499, 1200, 2500, 7500, 10000];

    showAlert(`Publishing ${count} simulated payment events to Kafka topic...`, 'success');

    for (let i = 0; i < count; i++) {
        const s = users[Math.floor(Math.random() * users.length)];
        let r = users[Math.floor(Math.random() * users.length)];
        while (r === s) {
            r = users[Math.floor(Math.random() * users.length)];
        }
        const amt = amounts[Math.floor(Math.random() * amounts.length)];
        const type = types[Math.floor(Math.random() * types.length)];

        await fetch(`${API_BASE}/api/transactions`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ senderId: s, receiverId: r, amount: amt, transactionType: type })
        });
    }

    setTimeout(() => {
        loadTransactions();
        loadAnalytics();
    }, 600);
}

function showAlert(msg, type) {
    const el = document.getElementById('alertBox');
    el.className = `alert alert-${type}`;
    const iconName = type === 'success' ? 'check_circle' : 'error';
    el.innerHTML = `<span class="material-symbols-outlined" style="font-size:20px;">${iconName}</span><span>${msg}</span>`;
    el.classList.remove('hidden');
    setTimeout(() => el.classList.add('hidden'), 5000);
}

function renderJsonPreview(obj) {
    const pre = document.getElementById('jsonPreview');
    const raw = JSON.stringify(obj, null, 2);
    // Simple light syntax highlighting
    pre.innerHTML = raw.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, match => {
        let style = 'color: #0f172a;';
        if (/^"/.test(match)) {
            if (/:$/.test(match)) {
                style = 'color: #2563eb; font-weight: 600;'; // Key
            } else {
                style = 'color: #059669;'; // String
            }
        } else if (/true|false/.test(match)) {
            style = 'color: #7c3aed; font-weight: 600;';
        } else if (/null/.test(match)) {
            style = 'color: #64748b; font-style: italic;';
        } else {
            style = 'color: #d97706; font-weight: 600;'; // Number
        }
        return `<span style="${style}">${match}</span>`;
    });
}

function animatePipeline() {
    const steps = [1, 2, 3, 4, 5];
    steps.forEach((step, idx) => {
        setTimeout(() => {
            document.querySelectorAll('.step-box').forEach(s => s.classList.remove('active'));
            const el = document.getElementById(`flowStep${step}`);
            if (el) el.classList.add('active');
        }, idx * 180);
    });
}

function formatTime(iso) {
    if (!iso) return '-';
    try {
        const d = new Date(iso);
        return d.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit' }) + ' ' + d.toLocaleDateString();
    } catch {
        return iso;
    }
}
