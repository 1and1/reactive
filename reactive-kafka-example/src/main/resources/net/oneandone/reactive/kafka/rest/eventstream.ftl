
<html>
    <head>

      </style>

       <#include "/net/oneandone/reactive/kafka/rest/css.ftl">
       
       
       
  
       <script type='text/javascript'>
            var type = typeof (window.EventSource);
            var idx = 0;

            if (typeof (window.EventSource) != 'undefined') {
                var source = new EventSource("${_requestURL}")
                source.onmessage = function(event) {
                        var ev = document.getElementById('events');
                        
                        var values = event.data.split('\n');
                        var type = values[0];
                        var data = values[1];
                        
                        var color
                        if (idx++%2 == 0) {
                            color = '#f8f8ff'
                        } else {
                            color = '#D6D6FF'
                        }
                        
                        ev.innerHTML += '<div id="log" style="background-color:' + color + '"> <font color="gray">' + type + '</font><br>' + data  + '</div>'


                        if (ev.childNodes.length > 55) {
                            ev.removeChild(ev.firstChild);
                        }
                };

                source.onerror = function(error) {
                    var ev = document.getElementById('events');
                    ev.innerHTML += '<div id="log">could not open Server-sent Events stream</div>'
                };

            } else {
                alert("your browser does not support HTML5 server-sent events");
            };
        </script>
    </head>


    <body>
        <div id="events" style="height: 500px; width: 10000px; font-family: Courier, 'Courier New', monospace; font-size: 0.7em;"></div>
    </body>
</html>
