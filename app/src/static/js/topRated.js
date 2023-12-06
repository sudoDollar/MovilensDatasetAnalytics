function filterData(filter) {
  // Add your logic here to filter the data based on the selected filter

  var buttons = document.querySelectorAll('.btn');
  buttons.forEach(function (button) {
      button.classList.remove('active');
  });

  // Add the 'active' class to the clicked button
  var clickedButton = document.getElementById(filter + 'Btn');
  clickedButton.classList.add('active');
  var url = "/api/movieFilter?filter=" + filter;
  var xhr = new XMLHttpRequest();
  xhr.open("GET", url, true);
  xhr.onreadystatechange = function () {
      if (xhr.readyState === 4 && xhr.status === 200) {
          var filterData = JSON.parse(xhr.responseText);
          var dropdown = document.createElement("select");
          dropdown.id = "movieDropdown";
          dropdown.className = "form-select";

          filterData.forEach(function (data) {
              var option = document.createElement("option");
              option.value = data;
              option.textContent = data;
              dropdown.appendChild(option);
          });
          var container = document.querySelector("#dropdownContainer");
          if (filter == "") {
              container.innerHTML = "";
          } else {
              container.innerHTML = "Select " + filter + ": ";
              container.appendChild(dropdown);
          }

          cb(dropdown.value);
          // Trigger the function call to cb when dropdown values change
          dropdown.addEventListener("change", function () {
              cb(this.value); // Pass the dropdown value to the cb function
          });
      }
  };
  xhr.send();
}

function cb(filterVal = null, graph = 'topRated') {
    jQuery.getJSON({
      url: "/callback", data: { 'filter': filterVal, 'graph': graph }, success: function (result) {
        Plotly.newPlot('chart1', result, {})
      }
    });
  }

window.onload = function () {
    filterData('');
}