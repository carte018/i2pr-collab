/**
 * RhphStructureReadRfcExceptions.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package edu.duke.oit.idms.oracle.connectors.recon_service_directories.logic.mc_style.functions.soap.sap.document.sap_com;

public class RhphStructureReadRfcExceptions implements java.io.Serializable {
    private java.lang.String _value_;
    private static java.util.HashMap _table_ = new java.util.HashMap();

    // Constructor
    protected RhphStructureReadRfcExceptions(java.lang.String value) {
        _value_ = value;
        _table_.put(_value_,this);
    }

    public static final java.lang.String _WegidNotFound = "WegidNotFound";
    public static final java.lang.String _CatalogueProblem = "CatalogueProblem";
    public static final java.lang.String _RootNotFound = "RootNotFound";
    public static final RhphStructureReadRfcExceptions WegidNotFound = new RhphStructureReadRfcExceptions(_WegidNotFound);
    public static final RhphStructureReadRfcExceptions CatalogueProblem = new RhphStructureReadRfcExceptions(_CatalogueProblem);
    public static final RhphStructureReadRfcExceptions RootNotFound = new RhphStructureReadRfcExceptions(_RootNotFound);
    public java.lang.String getValue() { return _value_;}
    public static RhphStructureReadRfcExceptions fromValue(java.lang.String value)
          throws java.lang.IllegalArgumentException {
        RhphStructureReadRfcExceptions enumeration = (RhphStructureReadRfcExceptions)
            _table_.get(value);
        if (enumeration==null) throw new java.lang.IllegalArgumentException();
        return enumeration;
    }
    public static RhphStructureReadRfcExceptions fromString(java.lang.String value)
          throws java.lang.IllegalArgumentException {
        return fromValue(value);
    }
    public boolean equals(java.lang.Object obj) {return (obj == this);}
    public int hashCode() { return toString().hashCode();}
    public java.lang.String toString() { return _value_;}
    public java.lang.Object readResolve() throws java.io.ObjectStreamException { return fromValue(_value_);}
    public static org.apache.axis.encoding.Serializer getSerializer(
           java.lang.String mechType, 
           java.lang.Class _javaType,  
           javax.xml.namespace.QName _xmlType) {
        return 
          new org.apache.axis.encoding.ser.EnumSerializer(
            _javaType, _xmlType);
    }
    public static org.apache.axis.encoding.Deserializer getDeserializer(
           java.lang.String mechType, 
           java.lang.Class _javaType,  
           javax.xml.namespace.QName _xmlType) {
        return 
          new org.apache.axis.encoding.ser.EnumDeserializer(
            _javaType, _xmlType);
    }
    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(RhphStructureReadRfcExceptions.class);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:sap-com:document:sap:soap:functions:mc-style", "RhphStructureRead.RfcExceptions"));
    }
    /**
     * Return type metadata object
     */
    public static org.apache.axis.description.TypeDesc getTypeDesc() {
        return typeDesc;
    }

}
