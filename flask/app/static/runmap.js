var NY = new google.maps.LatLng(40.739629, -73.988451);
map = null;
taxi = [];
markers = [];
rects = [];
flag1 = true;

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
  var r = [];
  var markerlist = [];

  tx = new google.maps.Marker({position: NY, zIndex: 100, icon: txicon});
  tx.setMap(map);
  taxi.push(tx);

  for (var j=0; j<9; j++) {
    r.push(new google.maps.Rectangle({
      strokeColor: '#FF0000',
      strokeOpacity: 0.8,
      strokeWeight: 2,
      fillColor: '#FF0000',
      fillOpacity: 0.35,
      map: map,
      bounds: {
        north: 33.685,
        south: 33.671,
        east: -116.234,
        west: -116.251
      }
    }));
  }
  rects.push(r);

  for (var i=0; i<3; i++) {
    marker = new google.maps.Marker({position: NY, zIndex: 10-i, icon: icons[markers.length % 8]});
    markerlist.push(marker);
    marker.setMap(map);
  }
  markers.push(markerlist);
}


function show_hide_markers() {
  var m;
  if (flag1) m = null;
  else       m = map;
  flag1 = !flag1;
  for (var j=0; j<markers.length; j++)
    for (var i=0; i<markers[j].length; i++)
      markers[j][i].setMap(m);
}


function update_values() {
    $.getJSON('/query',
        function(data) {
            vid     = data.vid;
            taxiloc = data.taxiloc;
            spots   = data.spots;
            corners = data.corners;

            if (vid.length > taxi.length) {
              for (var j = taxi.length; j < vid.length; j++) { create_markers(); }
            }

            for (var j = 0; j < taxi.length; j++) {
              taxi[j].setPosition(new google.maps.LatLng(taxiloc[j]));

              // var infowindow = new google.maps.InfoWindow({content: vid[j]});
              // taxi[j].addListener('click', function() {infowindow.open(map, taxi[j]);});

              for (var k=0; k<9; k++) {
                var xx = k%3;
                var yy = (k-xx)/3;
                rects[j][k].setOptions({bounds: {north: corners[j][0]+(yy-0)*0.005,
                                                 south: corners[j][0]+(yy-1)*0.005,
                                                 east:  corners[j][1]+(xx-0)*0.005,
                                                 west:  corners[j][1]+(xx-1)*0.005}});
              }

              for (var i=0; i<spots[j].length; i++) {
                markers[j][i].setPosition(new google.maps.LatLng(spots[j][i]));
              }
              for (var i=markers[j].length; i<spots[j].length; i++) {
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
