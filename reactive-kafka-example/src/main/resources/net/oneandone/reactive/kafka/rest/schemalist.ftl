

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

                <#list schemas?keys as type>
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
	
	    
	    <#include "/net/oneandone/reactive/kafka/rest/footer.ftl">
	</body>
</html>