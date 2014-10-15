clear;close all
%url
databaseurl='jdbc:sqlserver://202.121.129.201:1433;';
%driver
driver='com.microsoft.sqlserver.jdbc.SQLServerDriver'; 
username='dbviewer';   %登录名
password='sufefinlab';   %密码
databasename='JRTZ_ANA'; %数据源名称
conn = database(databasename,username,password,driver,databaseurl);
%conn=database('JRTZ_ANA','dbviewer','sufefinlab','com.microsoft.sqlserver.jdbc.SQLServerDriver','jdbc:sqlserver://202.121.129.201:1433;database=JRTZ_ANA');

if isconnection(conn)==1
    tic
    %QOT_D_BCK表为复权行情表，采用向前复权法
    %需要对齐的数据，利用指数日期来对齐，但后面需要处理空值问题
    %curs=exec(conn,'select IDX.PUB_DT,A.F0050 from (select * from QOT_D_BCK where SEC_CD=''600000'' and VAR_CL=''A'') A right join (select * from QOT_D_BCK where SEC_CD=''000001'' and VAR_CL=''Z'') IDX on A.PUB_DT=IDX.PUB_DT order by IDX.PUB_DT');
    %不需要对齐的数据，目前使用的JRTZ_ANA数据库已经对齐，停牌日价格等于上一日收盘价，成交量为0
    %curs=exec(conn,'select PUB_DT,F0050 from QOT_D_BCK where SEC_CD=''600000'' and VAR_CL=''A'' and PUB_DT>=''2010-1-1'' order by PUB_DT');

    %从当前目录读取代码表bank.csv读到cell中
    [bankname,bankticker]=textread('sh50.csv','%s%s','delimiter', ',');
    %把cell中的代码取出来
    banks=cell2mat(bankticker(2:end));

    %先取上证指数，确定数据长度
    startdate='2009-4-1';%起始日期
    stopdate='2012-9-30';%截止日期
    sqlstr=['select PUB_DT,F0050 from QOT_D_BCK where SEC_CD=''','000001',''' and VAR_CL=''Z'' and PUB_DT>=''',startdate,''' and PUB_DT<=''',stopdate,''' order by PUB_DT'];
    curs=exec(conn,sqlstr);
    curs=fetch(curs);
    data=curs.Data;
    idx=cell2mat(data(:,2));

    n=0;
    for i=1:length(banks)
        sqlstr=['select PUB_DT,F0050,F0070 from QOT_D_BCK where SEC_CD=''',banks(i,:),''' and VAR_CL=''A'' and PUB_DT>=''',startdate,''' and PUB_DT<=''',stopdate,''' order by PUB_DT'];
        curs=exec(conn,sqlstr);
        curs=fetch(curs);
        data=curs.Data;
        p=cell2mat(data(:,2));
        tp=cell2mat(data(:,3));
        %数据不足的股票跳过
        if length(p)~=length(idx)
            continue
        end
        n=n+1;
        price(:,n)=p;%日收盘价
        turnover(:,n)=tp;%日成交额
    end
    toc
    close(curs);      %关闭游标对象 
    covmat=cov(price);%协方差矩阵
    corrmat=corrcoef(price);%相关系数矩阵
end
close(conn);      %关闭数据库连接对象 
