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
    btn.innerHTML = '<span class="material-symbols-outlined btn-icon">sync</span><span>Streaming to Kafka...</span>';

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
            showAlert(`Payment streamed successfully. Transaction ID: ${result.transaction.transactionId} published to topic 'bank-transactions'.`, 'success');
            document.getElementById('jsonPreview').textContent = JSON.stringify(result.transaction, null, 2);

            // Reload table and stats
            setTimeout(() => {
                loadTransactions();
                loadAnalytics();
            }, 600);
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
                <td><strong>${txn.senderId}</strong></td>
                <td><strong>${txn.receiverId}</strong></td>
                <td>₹${parseFloat(txn.amount).toLocaleString('en-IN', { minimumFractionDigits: 2 })}</td>
                <td><span class="badge badge-${typeKey}">${txn.transactionType}</span></td>
                <td><span class="badge badge-success"><span class="material-symbols-outlined" style="font-size:12px;">check</span> ${txn.status}</span></td>
                <td><small>${formatTime(txn.timestamp)}</small></td>
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
            mongoText.textContent = 'Connected (27017)';
        } else {
            mongoBadge.className = 'status-badge error';
            mongoText.textContent = 'Disconnected';
        }

        document.getElementById('consumerCount').textContent = `${status.consumerProcessedCount || 0} msgs`;
    } catch (err) {
        console.warn('Status fetch error:', err);
    }
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
    }, 800);
}

function showAlert(msg, type) {
    const el = document.getElementById('alertBox');
    el.className = `alert alert-${type}`;
    const iconName = type === 'success' ? 'check_circle' : 'error';
    el.innerHTML = `<span class="material-symbols-outlined" style="font-size:18px;">${iconName}</span><span>${msg}</span>`;
    el.classList.remove('hidden');
    setTimeout(() => el.classList.add('hidden'), 5000);
}

function animatePipeline() {
    const steps = [1, 2, 3, 4, 5];
    steps.forEach((step, idx) => {
        setTimeout(() => {
            document.querySelectorAll('.step-box').forEach(s => s.classList.remove('active'));
            const el = document.getElementById(`flowStep${step}`);
            if (el) el.classList.add('active');
        }, idx * 200);
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
