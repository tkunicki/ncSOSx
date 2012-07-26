<?xml version="1.0"?>
<xsl:stylesheet version="1.0"
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fn="http://www.w3.org/2005/xpath-functions">

	<!-- This takes a parameter indicating the name of the secondary file to
		merge in. When we've matched the "web-app" element, we will first insert
		the "servlet" elements of the include file, then the "servlet" elements of
		the main file, then the "servlet-mapping" elements of the include file, then
		the "servlet-mapping" elements of the main file, and then all the remaining
		elements of the main file (which will be specified explicitly by element
		type. -->

	<xsl:output method="xml" indent="yes"/>

	<xsl:param name="includeFile"/>
	<xsl:param name="elementList">
		icon,display-name,description,distributable,context-param,filter,filter-mapping,listener,servlet,servlet-mapping,session-config,mime-mapping,welcome-file-list,error-page,taglib,resource-env-ref,resource-ref,security-constraint,login-config,security-role,env-entry,ejb-ref,ejb-local-ref
	</xsl:param>
	<xsl:param name="copyList">
		filter,filter-mapping,servlet,servlet-mapping
	</xsl:param>

	<xsl:template match="@*|node()">
		<xsl:copy>
			<xsl:apply-templates select="@*|node()"/>
		</xsl:copy>
	</xsl:template>

	<xsl:template match="*[local-name() = 'web-app']">
		<xsl:variable name="items" select="fn:tokenize(fn:normalize-space($elementList),',')"/>
		<xsl:variable name="this" select="."/>
		<xsl:variable name="that" select="document($includeFile)"/>
		<xsl:variable name="modifiedCopyList">,<xsl:value-of select="fn:normalize-space($copyList)"/>,</xsl:variable>
		<web-app>
			<xsl:for-each select="$items">
				<xsl:variable name="item" select="."/>
				<xsl:apply-templates select="$this/*[local-name()=$item]"/>
				<xsl:if test="fn:contains($modifiedCopyList,fn:concat(',',$item,','))">
					<xsl:apply-templates select="$that/*/*[local-name()=$item]"/>
				</xsl:if>
			</xsl:for-each>
		</web-app>
	</xsl:template>

</xsl:stylesheet>