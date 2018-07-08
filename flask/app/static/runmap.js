var NY = new google.maps.LatLng(40.739629, -73.988451);
map = null;
taxi = [];
markers = [];

txicon = 'http://maps.google.com/mapfiles/kml/pal2/icon47.png';
icons = ['http://maps.google.com/mapfiles/ms/icons/red-dot.png',
         'http://maps.google.com/mapfiles/ms/icons/blue-dot.png',
         'http://maps.google.com/mapfiles/ms/icons/yellow-dot.png',
         'http://maps.google.com/mapfiles/ms/icons/green-dot.png',
         'http://maps.google.com/mapfiles/ms/icons/orange-dot.png',
         'http://maps.google.com/mapfiles/ms/icons/purple-dot.png',
         'http://maps.google.com/mapfiles/ms/icons/ltblue-dot.png',
         'http://maps.google.com/mapfiles/ms/icons/pink-dot.png'];



function initialize() {
  var mapOptions = { zoom:   11,
                     center: NY };
  map = new google.maps.Map(document.getElementById('map-canvas'), mapOptions);
}


function create_markers() {
  var tx;
  var marker;
  var markerlist = [];

  tx = new google.maps.Marker({position: NY, zIndex: 100, icon: txicon});
  tx.setMap(null);
  taxi.push(tx);

  for (var i=0; i<3; i++) {
    marker = new google.maps.Marker({position: NY, zIndex: 10-i, icon: icons[markers.length % 8]});
    markerlist.push(marker);
    marker.setMap(null);
  }
  markers.push(markerlist);
}


function hide_markers(n) {
  taxi[n].setMap(null);
  for (var i=0; i<3; i++) { markers[n][i].setMap(null); }
}


function update_values() {
    $.getJSON('/query',
        function(data) {
            vid     = data.vid;
            taxiloc = data.taxiloc;
            spots   = data.spots;

            if (vid.length > taxi.length) {
              for (var j = taxi.length; j < vid.length; j++) { create_markers(); }
            }
            if (vid.length < taxi.length) {
              for (var j = vid.length; j < taxi.length; j++) { hide_markers(j);  }
            }

            for (var j = 0; j < taxi.length; j++) {
              taxi[j].setPosition(new google.maps.LatLng(taxiloc[j]));
              taxi[j].setMap(map);

              var infowindow = new google.maps.InfoWindow({content: vid[j]});
              taxi[j].addListener('click', function() {infowindow.open(map, taxi[j]);});

              for (var i=0; i<spots[j].length; i++) {
                markers[j][i].setPosition(new google.maps.LatLng(spots[j][i]));
                markers[j][i].setMap(map);
              }
              for (var i=spots[j].length; i<markers[j].length; i++) {
                markers[j][i].setMap(null);
              }
            }
        }
    );
}



window.setInterval(function(){ update_values() }, 750);

google.maps.event.addDomListener(window, 'load', initialize);
