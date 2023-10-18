create database pgz_test;
set password_encryption = 'md5';
create user pgz_user_md5 with encrypted password 'pg_user_md5_pw';
create user pgz_user_clear with password 'pg_user_clear_pw';
create user pgz_user_nopass;
