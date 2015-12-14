

<html>
	<head>
		<title>schemas</title>
		<#include "/net/oneandone/reactive/kafka/rest/css.ftl">
	</head>
	
	<body> 
	     
	    <table>
            <tbody>

                <tr>
                    <th>mimetype</th>
                    <th>description</th>
                </tr>

                <#list mimeTypes as type>
                <tr class="even">
                    <td><a href="schemas/${type?split("/")[0]}/${type?split("/")[1]?url}">${type}</a></td><td>${schemas[type]}</td>
                </tr>
                <tr class="odd">
                    <td>${type?replace("+json", ".list+json")}</a></td><td>
                    <#if (schemas[type]?length > 0)>                    
                    an array of ${schemas[type]}
                    </#if>
                    </td>
                </tr>
                </tr>
                </#list>
            </tbody>
        </table> 
	
	    <br>
	    <a href="erroneousschemas"><small>erroneous schemas</small></a>
	    
	    <#include "/net/oneandone/reactive/kafka/rest/footer.ftl">
	</body>
</html>