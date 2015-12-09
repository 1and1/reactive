

<html>
	<head>
		<title>business event service</title>
		<#include "/net/oneandone/reactive/kafka/rest/css.ftl">
	</head>
	
	<body> 
        <b>this is the busines event service</b> <br><br>
	     
	    <a href="${self}/topics">topics</a>   <br>
	    <a href="${base}/health">springboot health</a>  <br>  
	    <a href="${base}/trace">springboot trace</a>  <br>
	    <a href="${base}/metrics">springboot metrics</a> <br>
	    <a href="${base}/beans">springboot beans</a> <br>
	    <a href="${base}/autoconfig">springboot autoconfig</a> <br>
	    
	    
	    <#include "/net/oneandone/reactive/kafka/rest/footer.ftl">
	</body>
</html>