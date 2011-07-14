package edu.duke.oit.idms.oracle.connectors.prov_dynamic_groups;

import java.io.IOException;
import java.io.StringReader;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.jdom.xpath.XPath;

import Thor.API.tcResultSet;
import Thor.API.tcUtilityFactory;
import Thor.API.Operations.tcITResourceInstanceOperationsIntf;

import com.thortech.xl.dataaccess.tcDataProvider;

import edu.duke.oit.idms.oracle.provisioning.SimpleProvisioning;
import edu.duke.oit.idms.urn.FilterException;
import edu.duke.oit.idms.urn.URNFilterFactory;

/**
 * @author shilen
 */
public class GrouperWSWrapper {

  private static GrouperWSWrapper instance = null;

  private tcDataProvider dataProvider = null;
  
  // location for making WS calls to grouper
  private String grouperLocation = null;
  
  // grouper WS host
  private String grouperHost = null;
  
  // grouper WS port
  private int grouperPort;
  
  // grouper WS user
  private String grouperUser = null;
  
  // grouper WS password
  private String grouperPassword = null;
  
  // urn filters -- Map<String, URNFilter>
  private Map urnFilters = null;
  
  private GrouperWSWrapper(tcDataProvider dataProvider) {
    this.dataProvider = dataProvider;
    getProperties();
    this.urnFilters = getUrnFiltersDb();
    
    SimpleProvisioning.logger.info(DynamicGroupsProvisioning.connectorName + ": Created new instance of GrouperWSWrapper.");
  }

  /**
   * @param dataProvider 
   * @return new instance of this class
   */
  public static GrouperWSWrapper getInstance(tcDataProvider dataProvider) {
    if (instance == null) {
      instance = new GrouperWSWrapper(dataProvider);
    } else {
      instance.dataProvider = dataProvider;
    }

    return instance;
  }
  

  /**
   * Get connection properties
   */
  private void getProperties() {
    tcITResourceInstanceOperationsIntf moITResourceUtility = null;

    try {
      moITResourceUtility = (tcITResourceInstanceOperationsIntf) tcUtilityFactory
          .getUtility(dataProvider,
              "Thor.API.Operations.tcITResourceInstanceOperationsIntf");

      Map parameters = new HashMap();
      Map resourceMap = new HashMap();
      resourceMap.put("IT Resources.Name", "DYNGRP_PROVISIONING");
      tcResultSet moResultSet = moITResourceUtility.findITResourceInstances(resourceMap);
      long resourceKey = moResultSet.getLongValue("IT Resources.Key");

      moResultSet = null;
      moResultSet = moITResourceUtility.getITResourceInstanceParameters(resourceKey);
      for (int i = 0; i < moResultSet.getRowCount(); i++) {
        moResultSet.goToRow(i);
        String name = moResultSet.getStringValue("IT Resources Type Parameter.Name");
        String value = moResultSet
            .getStringValue("IT Resources Type Parameter Value.Value");
        parameters.put(name, value);
      }

      this.grouperLocation = (String)parameters.get("grouper-ws.location");
      this.grouperHost = (String)parameters.get("grouper-ws.host");
      this.grouperPort = Integer.parseInt((String)parameters.get("grouper-ws.port"));
      this.grouperUser = (String)parameters.get("grouper.user");
      this.grouperPassword = (String)parameters.get("grouper.password");
    } catch (Exception e) {
      throw new RuntimeException("Failed while reading IT resource: " + e.getMessage(), e);
    } finally {
      if (moITResourceUtility != null) {
        moITResourceUtility.close();
      }
    }

  }
  
  private HttpClient getHttpClient() {
    HttpClient httpClient = new HttpClient();
    Credentials defaultcreds = new UsernamePasswordCredentials(grouperUser, grouperPassword);
    httpClient.getState().setCredentials(new AuthScope(grouperHost, grouperPort), defaultcreds);

    return httpClient;
  }
  
  public Map getUrnFiltersDb() {
    
    Map urnFilters = new HashMap();
    HttpClient httpClient = this.getHttpClient();
    
    try {
      PutMethod method = new PutMethod(grouperLocation + "groups");
  
      // create a request to get all dynamic groups.
      String requestDocument = "<WsRestFindGroupsLiteRequest>" +
                                 "<queryFilterType>FIND_BY_EXACT_ATTRIBUTE</queryFilterType>" +
                                 "<groupAttributeName>isDynamic</groupAttributeName>" +
                                 "<groupAttributeValue>true</groupAttributeValue>" +
                                 "<actAsSubjectId>GrouperSystem</actAsSubjectId>" +
                                 "<includeGroupDetail>true</includeGroupDetail>" +
                               "</WsRestFindGroupsLiteRequest>";
  
      method.setRequestEntity(new StringRequestEntity(requestDocument, "text/xml", "UTF-8"));
      httpClient.executeMethod(method);
  
      Header successHeader = method.getResponseHeader("X-Grouper-success");
      String successString = successHeader == null ? null : successHeader.getValue();
      if (successString == null) {
        throw new RuntimeException("Failed to get list of dynamic groups.");
      }
      
      boolean success = "T".equals(successString);
      String response = method.getResponseBodyAsString();
      if (!success) {
        throw new RuntimeException("Failed to get list of dynamic: " + response);
      }
      
      SAXBuilder builder = new SAXBuilder();
      Document document = builder.build(new StringReader(response));
      Iterator groupIter = XPath.selectNodes(document, "//WsGroup").iterator();
      while (groupIter.hasNext()) {
        Element groupElement = (Element)groupIter.next();
        Element nameElement = groupElement.getChild("name");
        String groupName = nameElement.getText();

        String creator = groupElement.getChild("detail").getChild("createSubjectId").getText();

        // skip if somebody created a dynamic group using the Grouper UI or WS
        if (!creator.equals("GrouperSystem")) {
          DynamicGroupsProvisioning.logger.warn(DynamicGroupsProvisioning.connectorName + ": Skipping group that wasn't created by GrouperSystem " + groupName);
          continue;
        }

        int count = 0;
        int filterPosition = -1;
        Iterator attrNamesIter = groupElement.getChild("detail").getChild("attributeNames").getChildren().iterator();
        while (attrNamesIter.hasNext()) {
          Element attrNameElement = (Element)attrNamesIter.next();
          if (attrNameElement.getText().equals("filter")) {
            filterPosition = count;
            break;
          }
          count++;
        }

        List attrValues = groupElement.getChild("detail").getChild("attributeValues").getChildren();
        Element attrValueElement = (Element)attrValues.get(filterPosition);

        // save the filter and attribute associated with the dynamic group.
        String filter = attrValueElement.getValue();
        urnFilters.put(groupName, URNFilterFactory.parseFilter(filter));
        DynamicGroupsProvisioning.logger.info(DynamicGroupsProvisioning.connectorName + ": Found group " + groupName);
      }
    
    } catch (HttpException e) {
      throw new RuntimeException("Failed to get list of dynamic: " + e.getMessage(), e);
    } catch (IOException e) {
      throw new RuntimeException("Failed to get list of dynamic: " + e.getMessage(), e);
    } catch (FilterException e) {
      throw new RuntimeException("Failed to parse one or more filters: " + e.getMessage(), e);
    } catch (JDOMException e) {
      throw new RuntimeException("Failed to get list of dynamic: " + e.getMessage(), e);
    }

    return urnFilters;
  }
  
  public Map getUrnFilters() {
    return this.urnFilters;
  }
  
  public void addMember(String group, String dukeid) {
    try {
      HttpClient httpClient = this.getHttpClient();
      PutMethod method = new PutMethod(grouperLocation + "groups/" + URLEncoder.encode(group, "UTF-8") + "/members/sources/jndiperson/subjectId/" + dukeid);

      String requestDocument = "<WsRestAddMemberLiteRequest><actAsSubjectId>GrouperSystem</actAsSubjectId></WsRestAddMemberLiteRequest>";
      method.setRequestEntity(new StringRequestEntity(requestDocument, "text/xml", "UTF-8"));
      httpClient.executeMethod(method);

      Header successHeader = method.getResponseHeader("X-Grouper-success");
      String successString = successHeader == null ? null : successHeader.getValue();
      if (successString == null) {
        throw new RuntimeException("addMember failed for " + dukeid);
      }
      boolean success = "T".equals(successString);
      String response = method.getResponseBodyAsString();

      if (!success) {
        throw new RuntimeException("addMember failed for " + dukeid + ": " + response);
      }
    } catch (IOException e) {
      throw new RuntimeException("IOException during addMember with group " + group + " and dukeid " + dukeid + ": " + e.toString());
    }

  }

  public void deleteMember(String group, String dukeid) {
    try {
      HttpClient httpClient = this.getHttpClient();
      PutMethod method = new PutMethod(grouperLocation + "groups/" + URLEncoder.encode(group, "UTF-8") + "/members/sources/jndiperson/subjectId/" + dukeid);

      String requestDocument = "<WsRestDeleteMemberLiteRequest><actAsSubjectId>GrouperSystem</actAsSubjectId></WsRestDeleteMemberLiteRequest>";
      method.setRequestEntity(new StringRequestEntity(requestDocument, "text/xml", "UTF-8"));
      httpClient.executeMethod(method);

      Header successHeader = method.getResponseHeader("X-Grouper-success");
      String successString = successHeader == null ? null : successHeader.getValue();
      if (successString == null) {
        throw new RuntimeException("deleteMember failed for " + dukeid);
      }
      boolean success = "T".equals(successString);
      String response = method.getResponseBodyAsString();

      if (!success) {
        throw new RuntimeException("deleteMember failed for " + dukeid + ": " + response);
      }
    } catch (IOException e) {
      throw new RuntimeException("IOException during deleteMeber with group " + group + " and dukeid " + dukeid + ": " + e.toString());
    }
  }

  
}
