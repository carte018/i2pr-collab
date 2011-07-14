package edu.duke.oit.idms.oracle.connectors.prov_exchange;

import java.io.IOException;
import java.io.StringReader;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.PostMethod;
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

/**
 * @author shilen
 */
public class ExchangeConnectionWrapper {

  private static ExchangeConnectionWrapper instance = null;

  private tcDataProvider dataProvider = null;
  
  private String exchangeLocation = null;

  private String exchangeHost = null;

  private int exchangePort;

  private String exchangeUser = null;

  private String exchangePassword = null;

  
  private ExchangeConnectionWrapper(tcDataProvider dataProvider) {
    this.dataProvider = dataProvider;
    cacheData();
    
    SimpleProvisioning.logger.info(ExchangeProvisioning.connectorName + ": Created new instance of ExchangeConnectionWrapper.");
  }

  /**
   * @param dataProvider 
   * @return new instance of this class
   */
  public static ExchangeConnectionWrapper getInstance(tcDataProvider dataProvider) {
    if (instance == null) {
      instance = new ExchangeConnectionWrapper(dataProvider);
    } else {
      instance.dataProvider = dataProvider;
    }

    return instance;
  }

  /**
   * Cache connection data
   */
  private void cacheData() {
    tcITResourceInstanceOperationsIntf moITResourceUtility = null;

    try {
      moITResourceUtility = (tcITResourceInstanceOperationsIntf) tcUtilityFactory
          .getUtility(dataProvider,
              "Thor.API.Operations.tcITResourceInstanceOperationsIntf");

      Map<String, String> parameters = new HashMap<String, String>();
      Map<String, String> resourceMap = new HashMap<String, String>();
      resourceMap.put("IT Resources.Name", "EXCHANGE_PROVISIONING");
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
      
      this.exchangeLocation = (String)parameters.get("exchange-ws.location");
      this.exchangeHost = (String)parameters.get("exchange-ws.host");
      this.exchangePort = Integer.parseInt((String)parameters.get("exchange-ws.port"));
      
      if (parameters.get("exchange-ws.user") != null) {
        this.exchangeUser = (String)parameters.get("exchange-ws.user");
        if (this.exchangeUser.equals("")) {
          this.exchangeUser = null;
        }
      }
      
      if (parameters.get("exchange-ws.password") != null) {
        this.exchangePassword = (String)parameters.get("exchange-ws.password");
        if (this.exchangePassword.equals("")) {
          this.exchangePassword = null;
        }
      }

    } catch (Exception e) {
      SimpleProvisioning.logger.info(ExchangeProvisioning.connectorName + ": Received Exception", e);
      throw new RuntimeException("Failed while getting Exchange connection information: " + e.getMessage(), e);
    } finally {
      if (moITResourceUtility != null) {
        moITResourceUtility.close();
      }
    }
  }
  
  private HttpClient getHttpClient() {
    HttpClient httpClient = new HttpClient();
    
    if (exchangeUser != null) {
      Credentials defaultcreds = new UsernamePasswordCredentials(exchangeUser, exchangePassword);
      httpClient.getState().setCredentials(new AuthScope(exchangeHost, exchangePort), defaultcreds);
      httpClient.getParams().setAuthenticationPreemptive(true);
    }
    
    return httpClient;
  }
  
  /**
   * @param netid
   * @param primarySMTP
   * @param activeDirectoryHostname
   */
  public void provision(String netid, String primarySMTP, String activeDirectoryHostname) {

    try {
      String data = "&" + URLEncoder.encode("netID", "UTF-8") + "=" + URLEncoder.encode(netid, "UTF-8");
      data += "&" + URLEncoder.encode("primarySMTP", "UTF-8") + "=" + URLEncoder.encode(primarySMTP, "UTF-8");
      data += "&" + URLEncoder.encode("activeDirectoryHostname", "UTF-8") + "=" + URLEncoder.encode(activeDirectoryHostname, "UTF-8");

      HttpClient httpClient = this.getHttpClient();
      PostMethod method = new PostMethod(exchangeLocation + "Provision");

      method.setRequestEntity(new StringRequestEntity(data, "application/x-www-form-urlencoded", null));
      int code = httpClient.executeMethod(method);
      String response = method.getResponseBodyAsString();
      
      if (code != 200 && code != 201) {
        throw new RuntimeException("Unexpected return code during provision for " + netid + ": " + code + ": " + response);
      }
      
      SAXBuilder builder = new SAXBuilder();
      Document document = builder.build(new StringReader(response));
      XPath xpath = XPath.newInstance("//ns:Result/ns:Status");
      xpath.addNamespace("ns", "http://oit.duke.edu/exchange/admin");
      Element element = (Element)xpath.selectSingleNode(document);
      if (element == null || element.getText() == null) {
        throw new RuntimeException("No return status during provision for " + netid + ": " + response);
      }
      
      if (!element.getText().equals("Success")) {
        XPath xpath2 = XPath.newInstance("//ns:Result/ns:Reason");
        xpath2.addNamespace("ns", "http://oit.duke.edu/exchange/admin");
        Element element2 = (Element)xpath2.selectSingleNode(document);
        String reason = "";
        if (element2 != null) {
          reason = element2.getText();
        }
        
        throw new RuntimeException("Bad result during provision for " + netid + ".  Status=" + element.getText() + ", Reason=" + reason + ".");
      }

    } catch (IOException e) {
      throw new RuntimeException("IOException during provision for " + netid + ": " + e.getMessage(), e);
    } catch (JDOMException e) {
      throw new RuntimeException("JDOMException during provision for " + netid + ": " + e.getMessage(), e);
    }
  }

  /**
   * @param netid
   * @param activeDirectoryHostname
   */
  public void deprovision(String netid, String activeDirectoryHostname) {

    try {
      String data = "&" + URLEncoder.encode("netID", "UTF-8") + "=" + URLEncoder.encode(netid, "UTF-8");
      data += "&" + URLEncoder.encode("activeDirectoryHostname", "UTF-8") + "=" + URLEncoder.encode(activeDirectoryHostname, "UTF-8");

      HttpClient httpClient = this.getHttpClient();
      PostMethod method = new PostMethod(exchangeLocation + "Deprovision");

      method.setRequestEntity(new StringRequestEntity(data, "application/x-www-form-urlencoded", null));
      int code = httpClient.executeMethod(method);
      String response = method.getResponseBodyAsString();
      
      if (code != 200 && code != 201) {
        throw new RuntimeException("Unexpected return code during deprovision for " + netid + ": " + code + ": " + response);
      }
      
      SAXBuilder builder = new SAXBuilder();
      Document document = builder.build(new StringReader(response));
      XPath xpath = XPath.newInstance("//ns:Result/ns:Status");
      xpath.addNamespace("ns", "http://oit.duke.edu/exchange/admin");
      Element element = (Element)xpath.selectSingleNode(document);
      if (element == null || element.getText() == null) {
        throw new RuntimeException("No return status during deprovision for " + netid + ": " + response);
      }
      
      if (!element.getText().equals("Success")) {
        XPath xpath2 = XPath.newInstance("//ns:Result/ns:Reason");
        xpath2.addNamespace("ns", "http://oit.duke.edu/exchange/admin");
        Element element2 = (Element)xpath2.selectSingleNode(document);
        String reason = "";
        if (element2 != null) {
          reason = element2.getText();
        }
        
        throw new RuntimeException("Bad result during deprovision for " + netid + ".  Status=" + element.getText() + ", Reason=" + reason + ".");
      }

    } catch (IOException e) {
      throw new RuntimeException("IOException during deprovision for " + netid + ": " + e.getMessage(), e);
    } catch (JDOMException e) {
      throw new RuntimeException("JDOMException during deprovision for " + netid + ": " + e.getMessage(), e);
    }
  }
  
  /**
   * @param netid
   * @param forwardingAddress 
   * @param activeDirectoryHostname
   */
  public void setForwarding(String netid, String forwardingAddress, String activeDirectoryHostname) {

    try {
      String data = "&" + URLEncoder.encode("netID", "UTF-8") + "=" + URLEncoder.encode(netid, "UTF-8");
      data += "&" + URLEncoder.encode("forwardAddress", "UTF-8") + "=" + URLEncoder.encode(forwardingAddress, "UTF-8");
      data += "&" + URLEncoder.encode("deliverAndForward", "UTF-8") + "=" + URLEncoder.encode("true", "UTF-8");
      data += "&" + URLEncoder.encode("activeDirectoryHostname", "UTF-8") + "=" + URLEncoder.encode(activeDirectoryHostname, "UTF-8");

      HttpClient httpClient = this.getHttpClient();
      PostMethod method = new PostMethod(exchangeLocation + "SetForwarding");

      method.setRequestEntity(new StringRequestEntity(data, "application/x-www-form-urlencoded", null));
      int code = httpClient.executeMethod(method);
      String response = method.getResponseBodyAsString();
      
      if (code != 200 && code != 201) {
        throw new RuntimeException("Unexpected return code during setForwarding for " + netid + ": " + code + ": " + response);
      }
      
      SAXBuilder builder = new SAXBuilder();
      Document document = builder.build(new StringReader(response));
      XPath xpath = XPath.newInstance("//ns:Result/ns:Status");
      xpath.addNamespace("ns", "http://oit.duke.edu/exchange/admin");
      Element element = (Element)xpath.selectSingleNode(document);
      if (element == null || element.getText() == null) {
        throw new RuntimeException("No return status during setForwarding for " + netid + ": " + response);
      }
      
      if (!element.getText().equals("Success")) {
        XPath xpath2 = XPath.newInstance("//ns:Result/ns:Reason");
        xpath2.addNamespace("ns", "http://oit.duke.edu/exchange/admin");
        Element element2 = (Element)xpath2.selectSingleNode(document);
        String reason = "";
        if (element2 != null) {
          reason = element2.getText();
        }
        
        throw new RuntimeException("Bad result during setForwarding for " + netid + ".  Status=" + element.getText() + ", Reason=" + reason + ".");
      }

    } catch (IOException e) {
      throw new RuntimeException("IOException during setForwarding for " + netid + ": " + e.getMessage(), e);
    } catch (JDOMException e) {
      throw new RuntimeException("JDOMException during setForwarding for " + netid + ": " + e.getMessage(), e);
    }
  }
  
  /**
   * @param netid
   * @param activeDirectoryHostname
   */
  public void removeForwarding(String netid, String activeDirectoryHostname) {

    try {
      String data = "&" + URLEncoder.encode("netID", "UTF-8") + "=" + URLEncoder.encode(netid, "UTF-8");
      data += "&" + URLEncoder.encode("activeDirectoryHostname", "UTF-8") + "=" + URLEncoder.encode(activeDirectoryHostname, "UTF-8");

      HttpClient httpClient = this.getHttpClient();
      PostMethod method = new PostMethod(exchangeLocation + "RemoveForwarding");

      method.setRequestEntity(new StringRequestEntity(data, "application/x-www-form-urlencoded", null));
      int code = httpClient.executeMethod(method);
      String response = method.getResponseBodyAsString();
      
      if (code != 200 && code != 201) {
        throw new RuntimeException("Unexpected return code during removeForwarding for " + netid + ": " + code + ": " + response);
      }
      
      SAXBuilder builder = new SAXBuilder();
      Document document = builder.build(new StringReader(response));
      XPath xpath = XPath.newInstance("//ns:Result/ns:Status");
      xpath.addNamespace("ns", "http://oit.duke.edu/exchange/admin");
      Element element = (Element)xpath.selectSingleNode(document);
      if (element == null || element.getText() == null) {
        throw new RuntimeException("No return status during removeForwarding for " + netid + ": " + response);
      }
      
      if (!element.getText().equals("Success")) {
        XPath xpath2 = XPath.newInstance("//ns:Result/ns:Reason");
        xpath2.addNamespace("ns", "http://oit.duke.edu/exchange/admin");
        Element element2 = (Element)xpath2.selectSingleNode(document);
        String reason = "";
        if (element2 != null) {
          reason = element2.getText();
        }
        
        throw new RuntimeException("Bad result during removeForwarding for " + netid + ".  Status=" + element.getText() + ", Reason=" + reason + ".");
      }

    } catch (IOException e) {
      throw new RuntimeException("IOException during removeForwarding for " + netid + ": " + e.getMessage(), e);
    } catch (JDOMException e) {
      throw new RuntimeException("JDOMException during removeForwarding for " + netid + ": " + e.getMessage(), e);
    }
  }
  
  /**
   * @param netid
   * @param userid 
   * @param accessRight 
   * @param activeDirectoryHostname
   */
  public void addMailboxPermission(String netid, String userid, String accessRight, String activeDirectoryHostname) {

    try {
      String data = "&" + URLEncoder.encode("netID", "UTF-8") + "=" + URLEncoder.encode(netid, "UTF-8");
      data += "&" + URLEncoder.encode("userID", "UTF-8") + "=" + URLEncoder.encode(userid, "UTF-8");
      data += "&" + URLEncoder.encode("accessRight", "UTF-8") + "=" + URLEncoder.encode(accessRight, "UTF-8");
      data += "&" + URLEncoder.encode("activeDirectoryHostname", "UTF-8") + "=" + URLEncoder.encode(activeDirectoryHostname, "UTF-8");

      HttpClient httpClient = this.getHttpClient();
      PostMethod method = new PostMethod(exchangeLocation + "AddMailboxPermission");

      method.setRequestEntity(new StringRequestEntity(data, "application/x-www-form-urlencoded", null));
      int code = httpClient.executeMethod(method);
      String response = method.getResponseBodyAsString();
      
      if (code != 200 && code != 201) {
        throw new RuntimeException("Unexpected return code during addMailboxPermission for " + netid + ": " + code + ": " + response);
      }
      
      SAXBuilder builder = new SAXBuilder();
      Document document = builder.build(new StringReader(response));
      XPath xpath = XPath.newInstance("//ns:Result/ns:Status");
      xpath.addNamespace("ns", "http://oit.duke.edu/exchange/admin");
      Element element = (Element)xpath.selectSingleNode(document);
      if (element == null || element.getText() == null) {
        throw new RuntimeException("No return status during addMailboxPermission for " + netid + ": " + response);
      }
      
      if (!element.getText().equals("Success")) {
        XPath xpath2 = XPath.newInstance("//ns:Result/ns:Reason");
        xpath2.addNamespace("ns", "http://oit.duke.edu/exchange/admin");
        Element element2 = (Element)xpath2.selectSingleNode(document);
        String reason = "";
        if (element2 != null) {
          reason = element2.getText();
        }
        
        throw new RuntimeException("Bad result during addMailboxPermission for " + netid + ".  Status=" + element.getText() + ", Reason=" + reason + ".");
      }

    } catch (IOException e) {
      throw new RuntimeException("IOException during addMailboxPermission for " + netid + ": " + e.getMessage(), e);
    } catch (JDOMException e) {
      throw new RuntimeException("JDOMException during addMailboxPermission for " + netid + ": " + e.getMessage(), e);
    }
  }
}
