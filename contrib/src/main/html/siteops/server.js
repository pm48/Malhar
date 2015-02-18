/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */

/**
 * Functions for drawing server load vs time chart.
 */

function RenderServerLoadTimeChart()
{  
  // create/delete rows 
  if (serverLoadTable.getNumberOfRows() < serverLoadDataPoints.length)
  {    
    var numRows = serverLoadDataPoints.length - serverLoadTable.getNumberOfRows();
    serverLoadTable.addRows(numRows);
  } else {
    for(var i=(serverLoadTable.getNumberOfRows()-1); i >= serverLoadDataPoints.length; i--)
    {
      serverLoadTable.removeRow(i);    
    }
  }

  // Populate data table with time/cost data points. 
  for(var i=0; i < serverLoadTable.getNumberOfRows(); i++)
  {
    serverLoadTable.setCell(i, 0, new Date(parseInt(serverLoadDataPoints[i].timestamp)));
    serverLoadTable.setCell(i, 1, parseFloat(serverLoadDataPoints[i].view));
  }

  // get chart options
  var serverName = document.getElementById('servername').value;  
  var title = "All Servers (PVS/Min)";
  if (serverName != "all") title = serverName + " (PVS/Min)";
  var options = {pointSize: 0, lineWidth : 1, legend : { position : 'top'} };
  options.title = title;

  // Draw line chart.
  serverLoadChart.draw(serverLoadView, options); 
}

function DrawServerLoadTime()
{
  // get url 
  var url = "ServerLoad.php?from=" + Math.floor(serverLoadLookback);
  if ( serverName && (serverName.length > 0))
  {   
    url += "&server=" + serverName;    
  }

  // fetch data  
    try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {
        serverLoadData = connect.response;
        var pts = JSON.parse(serverLoadData);
        for(var i=0; i <  pts.length; i++) 
        {
          serverLoadDataPoints.push(pts[i]);
          delete pts[i];
        }
        delete pts;
        sortByKey(serverLoadDataPoints, "timestamp");
        RenderServerLoadTimeChart();
        delete serverLoadData;
        delete serverLoadDataPoints;
        serverLoadDataPoints = new Array();
      }
    }
    connect.open('GET',  url, true);
    connect.send(null);
  } catch(e) {
  }
  serverLoadLookback = (new Date().getTime()/1000) -  (3600*serverLoadInterval) - 60;
}

function HandleServerLoadTimeSubmit()
{
  // reset intercval  
  if(serverNowPlaying) clearInterval(serverNowPlaying);

  // get params 
  serverName = document.getElementById('servername').value;
  serverLoadLookback = document.getElementById('serverloadlookback').value;
  if ( !serverLoadLookback || (serverLoadLookback == "")) {
    serverLoadLookback = (new Date().getTime()/1000) - 3600;
  }  else {
    serverLoadLookback = (new Date().getTime()/1000) - 3600 * serverLoadLookback;
  }

  // set from values  
  document.getElementById('servername').value = serverName;
  var lookback = document.getElementById('serverloadlookback').value;
  document.getElementById('serverloadlookback').value = lookback;
  serverLoadInterval = lookback;
       
  // darw server load/time chart  
  DrawServerLoadTime();
  serverNowPlaying = setInterval(DrawServerLoadTime, 60 * 1000); 
}
