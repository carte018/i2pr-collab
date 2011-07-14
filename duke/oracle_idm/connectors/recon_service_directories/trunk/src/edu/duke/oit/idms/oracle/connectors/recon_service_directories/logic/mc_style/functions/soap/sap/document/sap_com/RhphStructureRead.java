/**
 * RhphStructureRead.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package edu.duke.oit.idms.oracle.connectors.recon_service_directories.logic.mc_style.functions.soap.sap.document.sap_com;

public class RhphStructureRead  implements java.io.Serializable {
    private java.lang.String begda;

    private java.lang.String endda;

    private java.lang.String objid;

    private java.lang.String otype;

    private java.lang.String plvar;

    private java.lang.String pupInfo;

    private TableOfQcatStru struTab;

    private java.math.BigDecimal tdepth;

    private java.lang.String wegid;

    private java.lang.String withStext;

    public RhphStructureRead() {
    }

    public RhphStructureRead(
           java.lang.String begda,
           java.lang.String endda,
           java.lang.String objid,
           java.lang.String otype,
           java.lang.String plvar,
           java.lang.String pupInfo,
           TableOfQcatStru struTab,
           java.math.BigDecimal tdepth,
           java.lang.String wegid,
           java.lang.String withStext) {
           this.begda = begda;
           this.endda = endda;
           this.objid = objid;
           this.otype = otype;
           this.plvar = plvar;
           this.pupInfo = pupInfo;
           this.struTab = struTab;
           this.tdepth = tdepth;
           this.wegid = wegid;
           this.withStext = withStext;
    }


    /**
     * Gets the begda value for this RhphStructureRead.
     * 
     * @return begda
     */
    public java.lang.String getBegda() {
        return begda;
    }


    /**
     * Sets the begda value for this RhphStructureRead.
     * 
     * @param begda
     */
    public void setBegda(java.lang.String begda) {
        this.begda = begda;
    }


    /**
     * Gets the endda value for this RhphStructureRead.
     * 
     * @return endda
     */
    public java.lang.String getEndda() {
        return endda;
    }


    /**
     * Sets the endda value for this RhphStructureRead.
     * 
     * @param endda
     */
    public void setEndda(java.lang.String endda) {
        this.endda = endda;
    }


    /**
     * Gets the objid value for this RhphStructureRead.
     * 
     * @return objid
     */
    public java.lang.String getObjid() {
        return objid;
    }


    /**
     * Sets the objid value for this RhphStructureRead.
     * 
     * @param objid
     */
    public void setObjid(java.lang.String objid) {
        this.objid = objid;
    }


    /**
     * Gets the otype value for this RhphStructureRead.
     * 
     * @return otype
     */
    public java.lang.String getOtype() {
        return otype;
    }


    /**
     * Sets the otype value for this RhphStructureRead.
     * 
     * @param otype
     */
    public void setOtype(java.lang.String otype) {
        this.otype = otype;
    }


    /**
     * Gets the plvar value for this RhphStructureRead.
     * 
     * @return plvar
     */
    public java.lang.String getPlvar() {
        return plvar;
    }


    /**
     * Sets the plvar value for this RhphStructureRead.
     * 
     * @param plvar
     */
    public void setPlvar(java.lang.String plvar) {
        this.plvar = plvar;
    }


    /**
     * Gets the pupInfo value for this RhphStructureRead.
     * 
     * @return pupInfo
     */
    public java.lang.String getPupInfo() {
        return pupInfo;
    }


    /**
     * Sets the pupInfo value for this RhphStructureRead.
     * 
     * @param pupInfo
     */
    public void setPupInfo(java.lang.String pupInfo) {
        this.pupInfo = pupInfo;
    }


    /**
     * Gets the struTab value for this RhphStructureRead.
     * 
     * @return struTab
     */
    public TableOfQcatStru getStruTab() {
        return struTab;
    }


    /**
     * Sets the struTab value for this RhphStructureRead.
     * 
     * @param struTab
     */
    public void setStruTab(TableOfQcatStru struTab) {
        this.struTab = struTab;
    }


    /**
     * Gets the tdepth value for this RhphStructureRead.
     * 
     * @return tdepth
     */
    public java.math.BigDecimal getTdepth() {
        return tdepth;
    }


    /**
     * Sets the tdepth value for this RhphStructureRead.
     * 
     * @param tdepth
     */
    public void setTdepth(java.math.BigDecimal tdepth) {
        this.tdepth = tdepth;
    }


    /**
     * Gets the wegid value for this RhphStructureRead.
     * 
     * @return wegid
     */
    public java.lang.String getWegid() {
        return wegid;
    }


    /**
     * Sets the wegid value for this RhphStructureRead.
     * 
     * @param wegid
     */
    public void setWegid(java.lang.String wegid) {
        this.wegid = wegid;
    }


    /**
     * Gets the withStext value for this RhphStructureRead.
     * 
     * @return withStext
     */
    public java.lang.String getWithStext() {
        return withStext;
    }


    /**
     * Sets the withStext value for this RhphStructureRead.
     * 
     * @param withStext
     */
    public void setWithStext(java.lang.String withStext) {
        this.withStext = withStext;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof RhphStructureRead)) return false;
        RhphStructureRead other = (RhphStructureRead) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.begda==null && other.getBegda()==null) || 
             (this.begda!=null &&
              this.begda.equals(other.getBegda()))) &&
            ((this.endda==null && other.getEndda()==null) || 
             (this.endda!=null &&
              this.endda.equals(other.getEndda()))) &&
            ((this.objid==null && other.getObjid()==null) || 
             (this.objid!=null &&
              this.objid.equals(other.getObjid()))) &&
            ((this.otype==null && other.getOtype()==null) || 
             (this.otype!=null &&
              this.otype.equals(other.getOtype()))) &&
            ((this.plvar==null && other.getPlvar()==null) || 
             (this.plvar!=null &&
              this.plvar.equals(other.getPlvar()))) &&
            ((this.pupInfo==null && other.getPupInfo()==null) || 
             (this.pupInfo!=null &&
              this.pupInfo.equals(other.getPupInfo()))) &&
            ((this.struTab==null && other.getStruTab()==null) || 
             (this.struTab!=null &&
              this.struTab.equals(other.getStruTab()))) &&
            ((this.tdepth==null && other.getTdepth()==null) || 
             (this.tdepth!=null &&
              this.tdepth.equals(other.getTdepth()))) &&
            ((this.wegid==null && other.getWegid()==null) || 
             (this.wegid!=null &&
              this.wegid.equals(other.getWegid()))) &&
            ((this.withStext==null && other.getWithStext()==null) || 
             (this.withStext!=null &&
              this.withStext.equals(other.getWithStext())));
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
        if (getBegda() != null) {
            _hashCode += getBegda().hashCode();
        }
        if (getEndda() != null) {
            _hashCode += getEndda().hashCode();
        }
        if (getObjid() != null) {
            _hashCode += getObjid().hashCode();
        }
        if (getOtype() != null) {
            _hashCode += getOtype().hashCode();
        }
        if (getPlvar() != null) {
            _hashCode += getPlvar().hashCode();
        }
        if (getPupInfo() != null) {
            _hashCode += getPupInfo().hashCode();
        }
        if (getStruTab() != null) {
            _hashCode += getStruTab().hashCode();
        }
        if (getTdepth() != null) {
            _hashCode += getTdepth().hashCode();
        }
        if (getWegid() != null) {
            _hashCode += getWegid().hashCode();
        }
        if (getWithStext() != null) {
            _hashCode += getWithStext().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(RhphStructureRead.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:sap-com:document:sap:soap:functions:mc-style", ">RhphStructureRead"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("begda");
        elemField.setXmlName(new javax.xml.namespace.QName("", "Begda"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("endda");
        elemField.setXmlName(new javax.xml.namespace.QName("", "Endda"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("objid");
        elemField.setXmlName(new javax.xml.namespace.QName("", "Objid"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("otype");
        elemField.setXmlName(new javax.xml.namespace.QName("", "Otype"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("plvar");
        elemField.setXmlName(new javax.xml.namespace.QName("", "Plvar"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("pupInfo");
        elemField.setXmlName(new javax.xml.namespace.QName("", "PupInfo"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("struTab");
        elemField.setXmlName(new javax.xml.namespace.QName("", "StruTab"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:sap-com:document:sap:soap:functions:mc-style", "TableOfQcatStru"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("tdepth");
        elemField.setXmlName(new javax.xml.namespace.QName("", "Tdepth"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "decimal"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("wegid");
        elemField.setXmlName(new javax.xml.namespace.QName("", "Wegid"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("withStext");
        elemField.setXmlName(new javax.xml.namespace.QName("", "WithStext"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
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
