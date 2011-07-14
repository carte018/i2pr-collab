package edu.duke.oit.idms.oracle.connectors.recon_service_directories;

import java.util.ArrayList;
import java.util.List;


/**
 * @author shilen
 *
 */
public class PersonRegistryAttribute {

  private String urn;
  private boolean isModifying = false;
  private boolean removeAllValues = false;
  private List<String> addValues = new ArrayList<String>();
  private List<String> removeValues = new ArrayList<String>();
  
  /**
   * @param urn
   */
  public PersonRegistryAttribute(String urn) {
    this.urn = urn;
  }
  
  /**
   * @return urn
   */
  public String getUrn() {
    return urn;
  }

  /**
   * @param removeAllValues
   */
  public void setRemoveAllValues(boolean removeAllValues) {
    this.removeAllValues = removeAllValues;
    if (this.removeAllValues) {
      this.isModifying = true;
    }
  }

  /**
   * @return boolean
   */
  public boolean isRemoveAllValues() {
    return removeAllValues;
  }
  
  
  /**
   * @return boolean
   */
  public boolean isModifying() {
    return this.isModifying;
  }
  
  /**
   * @param value
   */
  public void addValueToAdd(String value) {
    this.addValues.add(value);
    this.isModifying = true;
  }
  
  /**
   * @param values
   */
  public void setValuesToAdd(List<String> values) {
    this.addValues = new ArrayList<String>();
    this.addValues.addAll(values);
    this.isModifying = true;
  }
  
  /**
   * @param values
   */
  public void addAllValuesToAdd(List<String> values) {
    this.addValues.addAll(values);
    this.isModifying = true;
  }
  
  /**
   * @param value
   */
  public void addValueToRemove(String value) {
    this.removeValues.add(value);
    this.isModifying = true;
  }
  
  /**
   * @param values
   */
  public void setValuesToRemove(List<String> values) {
    this.removeValues = new ArrayList<String>();
    this.removeValues.addAll(values);
    this.isModifying = true;
  }
  
  /**
   * @param values
   */
  public void addAllValuesToRemove(List<String> values) {
    this.removeValues.addAll(values);
    this.isModifying = true;
  }
  
  /**
   * @return list
   */
  public List<String> getValuesToAdd() {
    return this.addValues;
  }
  
  /**
   * @return list
   */
  public List<String> getValuesToRemove() {
    return this.removeValues;
  }
}
