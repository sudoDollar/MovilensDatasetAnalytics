function cb(filterVal = null, graph = 'topRated') {
    jQuery.getJSON({
      url: "/callback", data: { 'filter': filterVal, 'graph': graph }, success: function (result) {
        Plotly.newPlot('chart1', result, {})
      }
    });
  }