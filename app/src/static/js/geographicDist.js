function getStateDistPieChart(state = 'AL') {
    console.log(state)
    jQuery.getJSON({
      url: "/getStateGenrePieChart", data: { 'state': state}, success: function (result) {
        Plotly.newPlot('pieChartState', result, {})
      }
    });
  }

  function getStateChart() {
    jQuery.getJSON({
      url: "/getStateChartDist", success: function (result) {
        Plotly.newPlot('stateChartDist', result, {})
      }
    });
  }


window.onload = function() {
    getStateDistPieChart();
    getStateChart();
};
