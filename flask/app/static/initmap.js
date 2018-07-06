var NY = new google.maps.LatLng(40.739629, -73.988451);


function initialize() {
  var mapOptions = { zoom: 13,
                     center: NY };
  var map = new google.maps.Map(document.getElementById('map-canvas'), mapOptions);

  var marker = new google.maps.Marker({position: NY,
                                       title:    "Insight Data Engineering"});
  marker.setMap(map);
}


google.maps.event.addDomListener(window, 'load', initialize);
