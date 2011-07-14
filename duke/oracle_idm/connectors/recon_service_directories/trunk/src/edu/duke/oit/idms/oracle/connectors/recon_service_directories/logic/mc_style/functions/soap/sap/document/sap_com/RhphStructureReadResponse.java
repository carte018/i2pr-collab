/**
 * RhphStructureReadResponse.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package edu.duke.oit.idms.oracle.connectors.recon_service_directories.logic.mc_style.functions.soap.sap.document.sap_com;

public class RhphStructureReadResponse  implements java.io.Serializable {
    private TableOfQcatStru struTab;

    public RhphStructureReadResponse() {
    }

    public RhphStructureReadResponse(
           TableOfQcatStru struTab) {
           this.struTab = struTab;
    }


    /**
     * Gets the struTab value for this RhphStructureReadResponse.
     * 
     * @return struTab
     */
    public TableOfQcatStru getStruTab() {
        return struTab;
    }


    /**
     * Sets the struTab value for this RhphStructureReadResponse.
     * 
     * @param struTab
     */
    public void setStruTab(TableOfQcatStru struTab) {
        this.struTab = struTab;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof RhphStructureReadResponse)) return false;
        RhphStructureReadResponse other = (RhphStructureReadResponse) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.struTab==null && other.getStruTab()==null) || 
             (this.struTab!=null &&
              this.struTab.equals(other.getStruTab())));
        __equalsCalc = null;
        return _equals;
    }

    private boolean __hashCodeCalc = false;
    public synchronized int hashCode() {
        if (__hashCodeCalc) {
            return 0;
        }
        __hashCodeCalc = true;
        int _hashCode = 1;
        if (getStruTab() != null) {
            _hashCode += getStruTab().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(RhphStructureReadResponse.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:sap-com:document:sap:soap:functions:mc-style", ">RhphStructureReadResponse"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("struTab");
        elemField.setXmlName(new javax.xml.namespace.QName("", "StruTab"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:sap-com:document:sap:soap:functions:mc-style", "TableOfQcatStru"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
    }

    /**
     * Return type metadata object
     */
    public static org.apache.axis.description.TypeDesc getTypeDesc() {
        return typeDesc;
    }

    /**
     * Get Custom Serializer
     */
    public static org.apache.axis.encoding.Serializer getSerializer(
           java.lang.String mechType, 
           java.lang.Class _javaType,  
           javax.xml.namespace.QName _xmlType) {
        return 
          new  org.apache.axis.encoding.ser.BeanSerializer(
            _javaType, _xmlType, typeDesc);
    }

    /**
     * Get Custom Deserializer
     */
    public static org.apache.axis.encoding.Deserializer getDeserializer(
           java.lang.String mechType, 
           java.lang.Class _javaType,  
           javax.xml.namespace.QName _xmlType) {
        return 
          new  org.apache.axis.encoding.ser.BeanDeserializer(
            _javaType, _xmlType, typeDesc);
    }

}
