

<html>
	<head>
		<title>business event service</title>
		<#include "/net/oneandone/reactive/kafka/rest/css.ftl">
	</head>
	
	<body> 
        <b>this is the busines event service</b> <br><br>
	     
	    <table>
            <tbody>
                <tr class="odd">
                    <td> <a href="${self}topics">the avaialble topics</a> </td>
                </tr>
                <tr class="even">
                    <td> <a href="${base}health">springboot health</a> </td> 
                </tr>
                <tr class="odd">                    
                    <td> <a href="${base}trace">springboot health</a> </td> 
                </tr>
                <tr class="even">                    
                    <td> <a href="${base}metrics">springboot metrics</a> </td> 
                </tr>
                <tr class="odd">                    
                    <td> <a href="${base}beans">springboot beans</a> </td>  
                </tr>
                <tr class="even">                    
                    <td> <a href="${base}autoconfig">springboot autoconfig</a> </td>
                </tr>

            </tbody>
        </table> 
	
	    
	    <#include "/net/oneandone/reactive/kafka/rest/footer.ftl">
	</body>
</html>