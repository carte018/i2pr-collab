package edu.duke.oit.idms.oracle.connectors.prov_siss;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import edu.duke.oit.idms.oracle.util.ProvisioningData;


/**
 * @author shilen
 *
 */
public class ProvisioningDataImpl extends ProvisioningData {
  
  /**
   * connector name
   */
  public final static String connectorName = SISSProvisioning.connectorName;
  
  private static ProvisioningDataImpl cfg = null;
  
  private Set<String> studentAttributes = null;
  
  private Set<String> facultyAttributes = null;
  
  private Set<String> staffAttributes = null;

  private final String STUDENT_SYNC_PREFIX = "student.sync.attribute.";
  
  private final String FACULTY_SYNC_PREFIX = "faculty.sync.attribute.";
  
  private final String STAFF_SYNC_PREFIX = "staff.sync.attribute.";
  
  private ProvisioningDataImpl() {
    super(connectorName);
  }
  
  /**
   * Get instance of class.
   * @return ProvisioningDataImpl
   */
  @SuppressWarnings("unchecked")
  protected static ProvisioningDataImpl getInstance() {
    if (cfg == null) {
      cfg = new ProvisioningDataImpl();
      
      cfg.studentAttributes = new LinkedHashSet<String>();
      cfg.staffAttributes = new LinkedHashSet<String>();
      cfg.facultyAttributes = new LinkedHashSet<String>();
      
      Map<String, String> allProperties = cfg.getAllProperties();
      Iterator<String> iter = allProperties.keySet().iterator();
      while (iter.hasNext()) {
        String property = iter.next();
        String value = allProperties.get(property);
        if (value != null && value.equals("true")) { 
          if (property.startsWith(cfg.STAFF_SYNC_PREFIX)) {
            String attribute = property.substring(cfg.STAFF_SYNC_PREFIX.length());
            cfg.staffAttributes.add(attribute.toLowerCase());
          } else if (property.startsWith(cfg.FACULTY_SYNC_PREFIX)) {
            String attribute = property.substring(cfg.FACULTY_SYNC_PREFIX.length());
            cfg.facultyAttributes.add(attribute.toLowerCase());
          } else if (property.startsWith(cfg.STUDENT_SYNC_PREFIX)) {
            String attribute = property.substring(cfg.STUDENT_SYNC_PREFIX.length());
            cfg.studentAttributes.add(attribute.toLowerCase());
          }
        }
      }
    }
    
    return cfg;
  }
  
  public boolean isSyncAttribute(String attribute) {
    throw new RuntimeException("override not implemented.");
  }
  
  public Set<String> getSyncAttributes() {
    throw new RuntimeException("override not implemented.");
  }
  
  public String[] getAllAttributes() {
    throw new RuntimeException("override not implemented.");
  }
  
  /**
   * Get all attributes based on affiliation
   * @param includeStudent
   * @param includeFaculty
   * @param includeEmeritus
   * @param includeStaff
   * @return set
   */
  public Set<String> getAllAttributes(boolean includeStudent, boolean includeFaculty, boolean includeEmeritus, boolean includeStaff) {
    Set<String> attributes = new LinkedHashSet<String>();
    if (includeStudent) {
      attributes.addAll(this.studentAttributes);
    }
    
    if (includeFaculty || includeEmeritus) {
      attributes.addAll(this.facultyAttributes);
    }
    
    if (includeStaff) {
      attributes.addAll(this.staffAttributes);
    }
    
    return attributes;
  }
}
