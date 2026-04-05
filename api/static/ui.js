document.addEventListener('DOMContentLoaded', () => {
    const statusBadge = document.getElementById('status-badge');
    const expansionBadge = document.getElementById('expansion-badge');
    const expansionCount = document.getElementById('expansion-count');
    const currentTask = document.getElementById('current-task');
    const totalFacts = document.getElementById('total-facts');
    const unmappedCount = document.getElementById('unmapped-count');
    const coreMetrics = document.getElementById('core-metrics');
    const activityList = document.getElementById('activity-list');
    const factsList = document.getElementById('facts-list');
    
    const btnStart = document.getElementById('btn-start');
    const btnPause = document.getElementById('btn-pause');

    let isFetching = false;

    async function fetchStatus() {
        if (isFetching) return;
        isFetching = true;
        
        try {
            const resp = await fetch('/api/status');
            const data = await resp.json();
            
            updateUI(data);
        } catch (err) {
            console.error('Failed to fetch status:', err);
        } finally {
            isFetching = false;
        }
    }

    function updateUI(data) {
        // Status Badge
        const status = data.running_status || 'PAUSE';
        statusBadge.textContent = status;
        statusBadge.className = `badge ${status.toLowerCase()}`;
        
        // Expansion Badge
        const isExpanding = data.is_expanding_dictionary || false;
        expansionBadge.textContent = isExpanding ? 'ACTIVE' : 'IDLE';
        expansionBadge.className = `badge ${isExpanding ? 'active' : 'idle'}`;
        
        // Expansion Count
        expansionCount.textContent = data.last_expansion_count || 0;

        // Buttons
        if (status === 'RUNNING') {
            btnStart.disabled = true;
            btnPause.disabled = false;
        } else {
            btnStart.disabled = false;
            btnPause.disabled = true;
        }

        // Current Task
        currentTask.textContent = data.current_target || 'Idle';
        
        // Stats
        if (data.db_stats) {
            totalFacts.textContent = data.db_stats.total_facts || 0;
            unmappedCount.textContent = data.db_stats.unmapped_count || 0;
            coreMetrics.textContent = data.db_stats.core_metrics || 0;
            
            // Recent Facts
            if (data.db_stats.recent_facts) {
                factsList.innerHTML = data.db_stats.recent_facts.map(f => `<div class="fact-item">${f}</div>`).join('');
            }
        }

        // Activity Log
        if (data.recent_activity && data.recent_activity.length > 0) {
            activityList.innerHTML = data.recent_activity.map(a => `<div class="activity-item">${a}</div>`).join('');
        } else {
            activityList.innerHTML = '<div class="activity-item">No recent activity.</div>';
        }
    }

    async function sendControl(command) {
        try {
            const resp = await fetch('/api/control', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ command })
            });
            const result = await resp.json();
            console.log('Control sent:', result);
            fetchStatus(); // Refresh immediately
        } catch (err) {
            alert('Failed to send control command');
            console.error(err);
        }
    }

    btnStart.addEventListener('click', () => sendControl('START'));
    btnPause.addEventListener('click', () => sendControl('PAUSE'));

    // Polling
    fetchStatus();
    setInterval(fetchStatus, 3000);
});
