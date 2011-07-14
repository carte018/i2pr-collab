create index ldap_key_idx on usr (upper(usr_udf_ldapkey));
create index uid_idx on usr (upper(usr_udf_uid));
create index psemplid_idx on usr (usr_udf_psemplid);
create index psemplid2_idx on usr (upper(usr_udf_psemplid));
create index psuseralias_idx on usr (upper(usr_udf_psuseralias));
