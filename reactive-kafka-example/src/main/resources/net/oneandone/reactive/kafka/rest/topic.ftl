

<html>
	<head>
		<title>topic ${topicname}</title>
		<#include "/net/oneandone/reactive/kafka/rest/css.ftl">
	</head>
	
	<body> 
	     
	    <table>
            <tbody>
                <tr class="odd">
                    <td> <a href="${base}topics/${topicname}/events">events</a>
                </tr>
                <tr class="even">                    
                    <td> <a href="${base}topics/${topicname}/schemas">schemas</a>
                </tr>
        </table> 
	
	    
	    <#include "/net/oneandone/reactive/kafka/rest/footer.ftl">
	</body>
</html>