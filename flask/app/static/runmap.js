var NY = new google.maps.LatLng(40.739629, -73.988451);
taxi = null;
map = null;
markers = [];



function initialize() {
  var mapOptions = { zoom:   13,
                     center: NY };
  map = new google.maps.Map(document.getElementById('map-canvas'), mapOptions);

  var marker;
  taxi = new google.maps.Marker({position: NY,
                                 icon: 'http://maps.google.com/mapfiles/ms/icons/yellow-dot.png'});
  taxi.setMap(null);

  for (var i=1; i<4; i++) {
    marker = new google.maps.Marker({position: NY,
                                     icon: 'http://maps.google.com/mapfiles/ms/icons/red-dot.png'});
    markers.push(marker);
    marker.setMap(null);
  }
}


function update_values() {
    $.getJSON('/query',
        function(data) {
            vid     = data.vid;
            taxiloc = data.taxiloc;
            spots   = data.spots;

            taxi.setPosition(new google.maps.LatLng(taxiloc["lat"], taxiloc["lng"]));

            taxi.setMap(map);

            var infowindow = new google.maps.InfoWindow({content: vid});
            taxi.addListener('click', function() {infowindow.open(map, taxi);});

            for (var i=0; i<spots.length; i++) {
              markers[i].setPosition(new google.maps.LatLng(spots[i]["lat"], spots[i]["lng"]));
              markers[i].setMap(map);
            }
            for (var i=spots.length; i<markers.length; i++) {
              markers[i].setMap(null);
            }
        }
    );
}



window.setInterval(function(){ update_values() }, 1000);

google.maps.event.addDomListener(window, 'load', initialize);
