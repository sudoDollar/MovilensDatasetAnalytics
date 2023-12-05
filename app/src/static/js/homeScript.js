window.onload = function () {
  refreshTable();
};


function searchMovie() {
  var movie = document.getElementById("searchMovie").value;
  var url = "/api/movies?searchMovie=" + movie;
  var xhr = new XMLHttpRequest();
  xhr.open("GET", url, true);
  xhr.onreadystatechange = function () {
    if (xhr.readyState === 4 && xhr.status === 200) {
      // Update the table with the new data
      var movies = JSON.parse(xhr.responseText);

      var tableBody = document.querySelector("#table2 tbody");

      if (movies.length == 0) {
        tableBody.innerHTML = "<tr><td colspan='3'>No movies found</td></tr>";
      }
      else { tableBody.innerHTML = ""; }

      movies.forEach(function (movie) {
        var row = document.createElement("tr");
        var col1 = document.createElement("td");
        var col2 = document.createElement("td");
        var col3 = document.createElement("td");
        col1.textContent = movie["title"];
        col2.textContent = movie["rating"].toFixed(1);
        col3.textContent = movie["year"];
        row.appendChild(col1);
        row.appendChild(col2);
        row.appendChild(col3);
        tableBody.appendChild(row);
      });
    }
  };
  xhr.send();
}


function refreshTable() {
  // AJAX code to fetch new data and update the table
  var url = "/api/movies";

  // Make an AJAX request
  var xhr = new XMLHttpRequest();
  xhr.open("GET", url, true);
  xhr.onreadystatechange = function () {
    if (xhr.readyState === 4 && xhr.status === 200) {
      // Update the table with the new data
      var movies = JSON.parse(xhr.responseText);
      var tableBody = document.querySelector("#table1 tbody");
      tableBody.innerHTML = "";

      movies.forEach(function (movie) {
        var row = document.createElement("tr");
        var col1 = document.createElement("td");
        var col2 = document.createElement("td");
        var col3 = document.createElement("td");
        col1.textContent = movie["title"];
        col2.textContent = movie["rating"].toFixed(1);
        col3.textContent = movie["year"];
        row.appendChild(col1);
        row.appendChild(col2);
        row.appendChild(col3);
        tableBody.appendChild(row);
      });
    }
  };
  xhr.send();
}