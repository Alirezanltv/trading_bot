/**
 * Dashboard Common JavaScript
 * Shared functionality across dashboard pages
 */

// Initialize when document is loaded
document.addEventListener('DOMContentLoaded', function() {
    // Highlight active nav item
    highlightCurrentNavItem();
    
    // Initialize tooltips
    initializeTooltips();
    
    // Initialize SSE connection if on dashboard pages
    if (document.getElementById('dashboard-content')) {
        initializeSSE();
    }
});

/**
 * Highlight the current navigation item based on URL
 */
function highlightCurrentNavItem() {
    const currentPath = window.location.pathname;
    
    // Find all nav links
    const navLinks = document.querySelectorAll('.nav-link');
    
    // Loop through links and highlight current page
    navLinks.forEach(link => {
        if (currentPath === link.getAttribute('href')) {
            link.classList.add('active');
        } else {
            link.classList.remove('active');
        }
    });
}

/**
 * Initialize Bootstrap tooltips
 */
function initializeTooltips() {
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
}

/**
 * Initialize Server-Sent Events connection
 */
function initializeSSE() {
    // Create SSE connection
    const evtSource = new EventSource('/api/events');
    
    // Handle different event types
    evtSource.addEventListener('message', function(event) {
        const data = JSON.parse(event.data);
        
        // Call appropriate update function based on data type
        if (data.type === 'initial') {
            processInitialData(data);
        } else if (data.type === 'update') {
            processUpdateData(data);
        }
    });
    
    // Handle SSE errors with automatic reconnect
    evtSource.onerror = function() {
        console.error('EventSource connection failed, attempting to reconnect in 5s...');
        evtSource.close();
        
        setTimeout(function() {
            console.log('Reconnecting SSE...');
            initializeSSE();
        }, 5000);
    };
}

/**
 * Process initial data from SSE connection
 */
function processInitialData(data) {
    // Update system health indicators
    updateSystemHealth(data.system_health);
    
    // Update component data
    updateComponents(data.components);
    
    // Call page-specific functions if they exist
    if (typeof onInitialData === 'function') {
        onInitialData(data);
    }
}

/**
 * Process update data from SSE connection
 */
function processUpdateData(data) {
    // Update system health indicators
    updateSystemHealth(data.system_health);
    
    // Update component data with animation
    updateComponents(data.components, true);
    
    // Call page-specific functions if they exist
    if (typeof onUpdateData === 'function') {
        onUpdateData(data);
    }
}

/**
 * Update system health indicators
 */
function updateSystemHealth(health) {
    if (!health) return;
    
    // Update system status badge
    const statusBadge = document.getElementById('system-status-badge');
    if (statusBadge) {
        if (health.failed_components > 0) {
            statusBadge.className = 'badge rounded-pill bg-danger';
            statusBadge.innerHTML = '<i class="bi bi-x-circle"></i> Critical';
        } else if (health.critical_components > 0) {
            statusBadge.className = 'badge rounded-pill bg-warning';
            statusBadge.innerHTML = '<i class="bi bi-exclamation-circle"></i> Warning';
        } else if (health.degraded_components > 0) {
            statusBadge.className = 'badge rounded-pill bg-info';
            statusBadge.innerHTML = '<i class="bi bi-info-circle"></i> Degraded';
        } else {
            statusBadge.className = 'badge rounded-pill bg-success';
            statusBadge.innerHTML = '<i class="bi bi-check-circle"></i> Healthy';
        }
    }
    
    // Update system summary
    updateElement('total-components', health.total_components || 0);
    updateElement('healthy-components', health.healthy_components || 0);
    updateElement('degraded-components', health.degraded_components || 0);
    updateElement('critical-components', health.critical_components || 0);
    updateElement('failed-components', health.failed_components || 0);
}

/**
 * Update components data
 * @param {Object} components Components data object
 * @param {boolean} animate Whether to animate changes
 */
function updateComponents(components, animate = false) {
    if (!components) return;
    
    // Update component elements if they exist
    Object.keys(components).forEach(compId => {
        const comp = components[compId];
        const compElement = document.getElementById(`component-${compId}`);
        
        if (compElement) {
            // Update status and health
            const statusElement = compElement.querySelector('.component-status');
            const healthElement = compElement.querySelector('.component-health');
            
            if (statusElement) {
                statusElement.textContent = comp.status;
                statusElement.className = `component-status ${getStatusClass(comp.status)}`;
            }
            
            if (healthElement) {
                healthElement.textContent = comp.health;
                healthElement.className = `component-health ${getHealthClass(comp.health)}`;
            }
            
            // Add flash animation if requested
            if (animate) {
                compElement.classList.add('flash-update');
                setTimeout(() => {
                    compElement.classList.remove('flash-update');
                }, 1000);
            }
        }
    });
}

/**
 * Update an element's text content if it exists
 * @param {string} id Element ID
 * @param {any} value Value to set
 */
function updateElement(id, value) {
    const element = document.getElementById(id);
    if (element) {
        element.textContent = value;
    }
}

/**
 * Get CSS class for component status
 * @param {string} status Component status
 * @returns {string} CSS class
 */
function getStatusClass(status) {
    switch (status) {
        case 'initialized':
            return 'text-success';
        case 'degraded':
            return 'text-warning';
        case 'error':
            return 'text-danger';
        case 'stopped':
            return 'text-secondary';
        default:
            return 'text-primary';
    }
}

/**
 * Get CSS class for component health
 * @param {string} health Component health
 * @returns {string} CSS class
 */
function getHealthClass(health) {
    switch (health) {
        case 'healthy':
            return 'text-success';
        case 'degraded':
        case 'warning':
            return 'text-warning';
        case 'critical':
        case 'failed':
            return 'text-danger';
        default:
            return 'text-secondary';
    }
}

/**
 * Format date string to locale time
 * @param {string} dateStr ISO date string
 * @returns {string} Formatted time string
 */
function formatTime(dateStr) {
    if (!dateStr) return '';
    const date = new Date(dateStr);
    return date.toLocaleTimeString();
}

/**
 * Format date string to locale date and time
 * @param {string} dateStr ISO date string
 * @returns {string} Formatted date and time string
 */
function formatDateTime(dateStr) {
    if (!dateStr) return '';
    const date = new Date(dateStr);
    return date.toLocaleString();
}

/**
 * Create a health indicator element
 * @param {string} health Health status
 * @returns {string} HTML string
 */
function createHealthIndicator(health) {
    return `<span class="health-indicator health-${health}"></span>`;
}

/**
 * Make an API call with error handling
 * @param {string} url API endpoint
 * @param {string} method HTTP method
 * @param {Object} data Request data
 * @returns {Promise} Promise resolving to response data
 */
async function apiCall(url, method = 'GET', data = null) {
    const options = {
        method: method,
        headers: {
            'Content-Type': 'application/json'
        }
    };
    
    if (data && (method === 'POST' || method === 'PUT')) {
        options.body = JSON.stringify(data);
    }
    
    try {
        const response = await fetch(url, options);
        
        if (!response.ok) {
            throw new Error(`API error: ${response.status} ${response.statusText}`);
        }
        
        return await response.json();
    } catch (error) {
        console.error('API call failed:', error);
        throw error;
    }
}

/**
 * Show a toast notification
 * @param {string} title Toast title
 * @param {string} message Toast message
 * @param {string} type Notification type (success, error, warning, info)
 */
function showToast(title, message, type = 'info') {
    // Check if toast container exists, create if not
    let toastContainer = document.getElementById('toast-container');
    
    if (!toastContainer) {
        toastContainer = document.createElement('div');
        toastContainer.id = 'toast-container';
        toastContainer.className = 'toast-container position-fixed bottom-0 end-0 p-3';
        document.body.appendChild(toastContainer);
    }
    
    // Create toast element
    const toastId = 'toast-' + Date.now();
    const toastEl = document.createElement('div');
    toastEl.id = toastId;
    toastEl.className = 'toast';
    toastEl.setAttribute('role', 'alert');
    toastEl.setAttribute('aria-live', 'assertive');
    toastEl.setAttribute('aria-atomic', 'true');
    
    // Set header background based on type
    let headerClass = 'bg-info text-white';
    let icon = 'info-circle';
    
    switch (type) {
        case 'success':
            headerClass = 'bg-success text-white';
            icon = 'check-circle';
            break;
        case 'error':
            headerClass = 'bg-danger text-white';
            icon = 'x-circle';
            break;
        case 'warning':
            headerClass = 'bg-warning';
            icon = 'exclamation-triangle';
            break;
    }
    
    // Build toast HTML
    toastEl.innerHTML = `
        <div class="toast-header ${headerClass}">
            <i class="bi bi-${icon} me-2"></i>
            <strong class="me-auto">${title}</strong>
            <small>${formatTime(new Date().toISOString())}</small>
            <button type="button" class="btn-close btn-close-white" data-bs-dismiss="toast" aria-label="Close"></button>
        </div>
        <div class="toast-body">
            ${message}
        </div>
    `;
    
    // Add to container
    toastContainer.appendChild(toastEl);
    
    // Initialize and show toast
    const toast = new bootstrap.Toast(toastEl, {
        delay: 5000,
        autohide: true
    });
    
    toast.show();
    
    // Remove from DOM after hidden
    toastEl.addEventListener('hidden.bs.toast', function () {
        toastEl.remove();
    });
}

/**
 * Confirm action with modal
 * @param {string} title Modal title
 * @param {string} message Modal message
 * @param {string} confirmText Confirm button text
 * @param {string} confirmClass Confirm button class
 * @returns {Promise} Promise resolving to true if confirmed, false otherwise
 */
function confirmAction(title, message, confirmText = 'Confirm', confirmClass = 'btn-primary') {
    return new Promise((resolve) => {
        // Create modal element
        const modalId = 'confirm-modal-' + Date.now();
        const modalEl = document.createElement('div');
        modalEl.className = 'modal fade';
        modalEl.id = modalId;
        modalEl.setAttribute('tabindex', '-1');
        modalEl.setAttribute('aria-hidden', 'true');
        
        // Build modal HTML
        modalEl.innerHTML = `
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title">${title}</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <div class="modal-body">
                        <p>${message}</p>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                        <button type="button" class="btn ${confirmClass}" id="${modalId}-confirm">${confirmText}</button>
                    </div>
                </div>
            </div>
        `;
        
        // Add to body
        document.body.appendChild(modalEl);
        
        // Initialize modal
        const modal = new bootstrap.Modal(modalEl);
        
        // Handle confirm button
        const confirmBtn = document.getElementById(`${modalId}-confirm`);
        confirmBtn.addEventListener('click', function() {
            modal.hide();
            resolve(true);
        });
        
        // Handle modal hidden
        modalEl.addEventListener('hidden.bs.modal', function() {
            modalEl.remove();
            resolve(false);
        });
        
        // Show modal
        modal.show();
    });
} 