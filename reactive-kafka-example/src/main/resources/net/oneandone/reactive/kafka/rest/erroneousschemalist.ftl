

<html>
	<head>
		<title>schemas</title>
		<#include "/net/oneandone/reactive/kafka/rest/css.ftl">
	</head>
	
	<body> 
	     
	    <table>
            <tbody>
	
	    <br><br>
	        <table>
            <tbody>

                <tr>
                    <th>schema</th>
                    <th>error description</th>
                </tr>
      
                <#assign odd = true> 
                <#list erroneousSchemas as erroneousSchema>
                <#if odd>
                <tr class="odd">
                <#else>
                <tr class="even">
                </#if>
                <#assign odd = !odd>
                <tr class="even">
                    </td><td>${erroneousSchema.error}</td><td><pre>${erroneousSchema?html}</pre>
                </tr>
                </tr>
                </#list>
            </tbody>
        </table> 
	    
	    <#include "/net/oneandone/reactive/kafka/rest/footer.ftl">
	</body>
</html>