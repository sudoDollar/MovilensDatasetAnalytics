function getOccupationChart(occupation = 'academic/educator') {
    console.log(occupation)
    jQuery.getJSON({
      url: "/getGenreOccupationChart", data: { 'occupation': occupation}, success: function (result) {
        Plotly.newPlot('chartOccupation', result, {})
      }
    });
  }

  function getAgeChart(ageGroup = '6-10') {
    jQuery.getJSON({
      url: "/getGenreAgeChart", data: { 'ageGroup': ageGroup}, success: function (result) {
        Plotly.newPlot('chartAge', result, {})
      }
    });
  }


window.onload = function() {
    getOccupationChart();
    getAgeChart();
};
