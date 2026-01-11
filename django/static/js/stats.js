const chartColors = {
    primary: '#F5D76E',
    secondary: '#D4AC0D',
    background: 'rgba(245, 215, 110, 0.5)',
    border: '#D4AC0D'
};

function initGenreChart(labels, data) {
    new Chart(document.getElementById('genreChart'), {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Nombre de films',
                data: data,
                backgroundColor: chartColors.background,
                borderColor: chartColors.border,
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: { y: { beginAtZero: true } }
        }
    });
}

function initDecadeChart(labels, data) {
    new Chart(document.getElementById('decadeChart'), {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Nombre de films',
                data: data,
                fill: true,
                backgroundColor: chartColors.background,
                borderColor: chartColors.border,
                borderWidth: 2,
                tension: 0.3
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: { y: { beginAtZero: true } }
        }
    });
}

function initRatingChart(labels, data) {
    const ratingColors = [
        'rgba(231, 76, 60, 0.6)',
        'rgba(230, 126, 34, 0.6)',
        'rgba(241, 196, 15, 0.6)',
        'rgba(241, 196, 15, 0.6)',
        'rgba(245, 215, 110, 0.6)',
        'rgba(245, 215, 110, 0.6)',
        'rgba(46, 204, 113, 0.6)',
        'rgba(46, 204, 113, 0.6)',
        'rgba(39, 174, 96, 0.6)',
        'rgba(39, 174, 96, 0.6)',
        'rgba(39, 174, 96, 0.6)'
    ];

    new Chart(document.getElementById('ratingChart'), {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Nombre de films',
                data: data,
                backgroundColor: ratingColors,
                borderColor: chartColors.border,
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: { y: { beginAtZero: true } }
        }
    });
}

function initActorsChart(labels, data) {
    new Chart(document.getElementById('actorsChart'), {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Nombre de films',
                data: data,
                backgroundColor: chartColors.background,
                borderColor: chartColors.border,
                borderWidth: 2
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: { x: { beginAtZero: true } }
        }
    });
}
