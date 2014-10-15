clear;close all
%url
databaseurl='jdbc:sqlserver://202.121.129.201:1433;';
databaseurl2='jdbc:sqlserver://202.121.129.201:1433;database=txnfdb;';
%driver
driver='com.microsoft.sqlserver.jdbc.SQLServerDriver'; 
username='dbviewer';   %登录名
password='sufefinlab';   %密码
databasename='JRTZ_ANA'; %数据源名称
databasename2='txnfdb'; %数据源名称2
conn = database(databasename,username,password,driver,databaseurl);
conn2 = database(databasename2,username,password,driver,databaseurl2);
%conn=database('JRTZ_ANA','dbviewer','sufefinlab','com.microsoft.sqlserver.jdbc.SQLServerDriver','jdbc:sqlserver://202.121.129.201:1433;database=JRTZ_ANA');

if isconnection(conn)==1 && isconnection(conn2)==1
    tic
    
    %取行情
    %QOT_D_BCK表为复权行情表，采用向前复权法
    %需要对齐的数据，利用指数日期来对齐，但后面需要处理空值问题
    %curs=exec(conn,'select IDX.PUB_DT,A.F0050 from (select * from QOT_D_BCK where SEC_CD=''600000'' and VAR_CL=''A'') A right join (select * from QOT_D_BCK where SEC_CD=''000001'' and VAR_CL=''Z'') IDX on A.PUB_DT=IDX.PUB_DT order by IDX.PUB_DT');
    %不需要对齐的数据，目前使用的JRTZ_ANA数据库已经对齐，停牌日价格等于上一日收盘价，成交量为0
    %curs=exec(conn,'select PUB_DT,F0050 from QOT_D_BCK where SEC_CD=''600000'' and VAR_CL=''A'' and PUB_DT>=''2010-1-1'' order by PUB_DT');

    %从当前目录读取代码表hs300.csv读到cell中
    [bankname,bankticker]=textread('hs300.csv','%s%s','delimiter', ',');
    %把cell中的代码取出来
    banks=cell2mat(bankticker(2:end));

    %先取上证指数，确定数据长度
    %startdate='2009-4-1';%起始日期
    %sqlstr=['select PUB_DT,F0050 from QOT_D_BCK where SEC_CD=''','000300',''' and VAR_CL=''Z'' and PUB_DT>=''',startdate,''' order by PUB_DT'];
    startdate='20050408';%起始日期
    sqlstr=['select f_trade_date,f_close from stock.T_INDEX_TRADE_PUBLIC where F_INDEX_CODE=''399300'' and f_trade_date>=''',startdate,''' order by f_trade_date'];
    curs=exec(conn2,sqlstr);
    curs=fetch(curs);
    data=curs.Data;
    idx=cell2mat(data(:,2));

    n=0;
    d='';
    for i=1:length(banks)
        %sqlstr=['select PUB_DT,F0010,F0020,F0030,F0040,F0050,F0060,F0070 from QOT_D_BCK where SEC_CD=''',banks(i,:),''' and VAR_CL=''A'' and PUB_DT>=''',startdate,''' order by PUB_DT'];
        sqlstr=['select F_TRADE_DATE,F_PRECLOSE,F_OPEN,F_HIGH,F_LOW,F_CLOSE,isnull(F_VOLUME,0),isnull(F_AMOUNT,0),F_EXRIGHT from stock.T_STOCK_TRADE_EX where F_STOCK_CODE=''',banks(i,:),''' and f_trade_date in (select f_trade_date from stock.T_INDEX_TRADE_PUBLIC where f_index_code=''399300'') and F_TRADE_DATE>=''',startdate,''' order by F_TRADE_DATE'];
        curs=exec(conn2,sqlstr);
        curs=fetch(curs);
        data=curs.Data;
        p=cell2mat(data(:,2:9));
        if i==1
            d=cell2mat(data(:,1));
            datelist=datenum(fix(d/10000),fix(mod(d,10000)/100),mod(d,100));
        end
        %数据不足的股票跳过
        if length(p)~=length(idx)
            continue
        end
        %连续停牌超过60天的股票跳过
        %if length(find(diff(p(:,6))==0))>60
        %    continue
        %end
        n=n+1;
        datadaily(:,n,1)=p(:,1).*p(:,8);
        datadaily(:,n,2)=p(:,2).*p(:,8);
        datadaily(:,n,3)=p(:,3).*p(:,8);
        datadaily(:,n,4)=p(:,4).*p(:,8);
        datadaily(:,n,5)=p(:,5).*p(:,8);
        datadaily(:,n,6)=p(:,6);
        datadaily(:,n,7)=p(:,7);
        validstock(n,:)=banks(i,:);
    end
    close(curs);      %关闭游标对象 
    
    %120日平均交易额标准差（datadaily 8#）
    %25日平均交易额（datadaily 9#）
    %75日平均交易额（datadaily 10#）
    %120日平均交易额（datadaily 11#）
    %250日平均交易额（datadaily 12#）
    %25日平均收盘价（datadaily 13#）
    %75日平均收盘价（datadaily 14#）
    for i=120:length(datelist)
        datadaily(i,:,8)=std(datadaily(i-119:i,:,7));
    end
    for i=25:length(datelist)
        datadaily(i,:,9)=mean(datadaily(i-24:i,:,7));
        datadaily(i,:,13)=mean(datadaily(i-24:i,:,5));
    end
    for i=75:length(datelist)
        datadaily(i,:,10)=mean(datadaily(i-74:i,:,7));
        datadaily(i,:,14)=mean(datadaily(i-74:i,:,5));
    end
    for i=120:length(datelist)
        datadaily(i,:,11)=mean(datadaily(i-119:i,:,7));
    end
    for i=250:length(datelist)
        datadaily(i,:,12)=mean(datadaily(i-249:i,:,7));
    end
    
    %日行情转月行情
    %monthlist为月度列表
    monthlist=[2005,4,0];%[年,月,每月天数]
    n=1;% monthlist(n)目前处理的月度
    [Y, M, D, H, MN, S] = datevec(datelist);
    datamonthly(1,:,:)=datadaily(1,:,:);
    datamonthly(:,:,8:19)=0;
    idxmonthly(1,:)=idx(1);
    for i=2:length(datelist)
        if Y(i)~=monthlist(n,1) || M(i)~=monthlist(n,2)
            %新月份
            n=n+1;
            monthlist(n,:)=[Y(i), M(i),1];
            %赋月初值
            datamonthly(n,:,1: 7)=datadaily(i,:,1:7);
            idxmonthly(n,:)=idx(i);
        else
            for j=1:size(datadaily,2)
                %月最高价
                if datadaily(i,j,3)>datamonthly(n,j,3)
                    datamonthly(n,j,3)=datadaily(i,j,3);
                end
                %月最低价
                if datadaily(i,j,4)<datamonthly(n,j,4)
                    datamonthly(n,j,4)=datadaily(i,j,4);
                end
                %月最新价
                datamonthly(n,j,5)=datadaily(i,j,5);
                %成交量
                datamonthly(n,j,6)=datamonthly(n,j,6)+datadaily(i,j,6);
                %成交金额
                datamonthly(n,j,7)=datamonthly(n,j,7)+datadaily(i,j,7);
                %120日平均交易额标准差的和 datamonthly 15#
                datamonthly(n,j,15)=datamonthly(n,j,15)+datadaily(i,j,8);
                %25日/120日平均交易额的和 datamonthly 16#
                if datadaily(i,j,11)~=0
                    datamonthly(n,j,16)=datamonthly(n,j,16)+datadaily(i,j,9)./datadaily(i,j,11);
                end
                %75日/250日平均交易额的和 datamonthly 17#
                if datadaily(i,j,12)~=0
                    datamonthly(n,j,17)=datamonthly(n,j,17)+datadaily(i,j,10)./datadaily(i,j,12);
                end
                %（25日变动平均股价-前一天股价）/25日变动平均股价的和 18#
                if datadaily(i,j,9)~=0
                    datamonthly(n,j,18)=datamonthly(n,j,18)+(datadaily(i,j,13)-datadaily(i-1,j,5))./datadaily(i,j,13);
                end
                %（75日变动平均股价-前一天股价）/75日变动平均股价的和 19#
                if datadaily(i,j,10)~=0
                    datamonthly(n,j,19)=datamonthly(n,j,19)+(datadaily(i,j,14)-datadaily(i-1,j,5))./datadaily(i,j,14);
                end
            end
            idxmonthly(n,:)=idx(i);
            monthlist(n,3)=monthlist(n,3)+1;
        end
    end
   
    %月相关关系
    covmat=cov(datamonthly(:,:,5));%协方差矩阵
    corrmat=corrcoef(datamonthly(:,:,5));%相关系数矩阵

    %月对数收益率 datamonthly第8张表
    datamonthly(2:end,:,8)=diff(log(datamonthly(:,:,5)));
    idxmonthly(2:end,2)=diff(log(idxmonthly(:,1)));

    %因素1
    %求月beta 24个月移动窗口
    betamonthly=zeros(1,length(validstock));
    for i=24:length(monthlist)
        X(:,1)=ones(24,1);
        X(:,2)=idxmonthly(i-23:i,2);
        for j=1:length(validstock)
            [b,se_b,mse] = lscov(X,datamonthly(i-23:i,j,8));
            alphamonthly(i,j)=b(1);
            betamonthly(i,j)=b(2);
            epsilonmonthly(i,j)=datamonthly(i,j,8)-(alphamonthly(i,j)+betamonthly(i,j)*idxmonthly(i,2));
        end
        sigmamonthly(i,:)=std(datamonthly(i-23:i,:,8));
    end
    
    %日行情转周行情
    %weeklist为周度列表
    weeklist=[2005,1,4];%[Y,W,M]
    n=1;% weeklist(n)目前处理的周度
    [Y, M, D, H, MN, S] = datevec(datelist);
    dataweekly(1,:,:)=datadaily(1,:,1:7);
    idxweekly(1,:)=idx(1);
    W=fix((datelist-datenum('2005-4-4')+1)/7+1);%周数
    for i=2:length(datelist)
        if  W(i)~=weeklist(n,2)
            %新周
            n=n+1;
            weeklist(n,:)=[Y(i), W(i),M(i)];
            %赋周初值
            dataweekly(n,:,1:7)=datadaily(i,:,1:7);
            idxweekly(n,:)=idx(i);
        else
            for j=1:size(datadaily,2)
                %周最高价
                if datadaily(i,j,3)>dataweekly(n,j,3)
                    dataweekly(n,j,3)=datadaily(i,j,3);
                end
                %周最低价
                if datadaily(i,j,4)<dataweekly(n,j,4)
                    dataweekly(n,j,4)=datadaily(i,j,4);
                end
                %周最新价
                dataweekly(n,j,5)=datadaily(i,j,5);
                %成交量
                dataweekly(n,j,6)=dataweekly(n,j,6)+datadaily(i,j,6);
                %成交金额
                dataweekly(n,j,7)=dataweekly(n,j,7)+datadaily(i,j,7);
            end
            idxweekly(n,:)=idx(i);
        end
    end    
   
    %周对数收益率 dataweekly第8张表
    dataweekly(2:end,:,8)=diff(log(dataweekly(:,:,5)));
    idxweekly(2:end,2)=diff(log(idxweekly(:,1)));

    %因素2
    %求周beta 52周移动窗口
    betaweekly=zeros(1,length(validstock));
    for i=52:length(weeklist)
        XW(:,1)=ones(52,1);
        XW(:,2)=idxweekly(i-51:i,2);
        for j=1:length(validstock)
            [b,se_b,mse] = lscov(XW,dataweekly(i-51:i,j,8));
            alphaweekly(i,j)=b(1);
            betaweekly(i,j)=b(2);
            epsilonweekly(i,j)=dataweekly(i,j,8)-(alphaweekly(i,j)+betaweekly(i,j)*idxweekly(i,2));
        end
        sigmaweekly(i,:)=std(dataweekly(i-51:i,:,8));
    end
    %求月度周平均beta、alpha、标准差、残差
    wb=zeros(length(monthlist),length(validstock),2);%wb(sum of beta,count of beta);
    wa=zeros(length(monthlist),length(validstock),2);%wa(sum of alpha,count of alpha);
    ws=zeros(length(monthlist),length(validstock),2);%ws(sum of sigma,count of sigma);
    we=zeros(length(monthlist),length(validstock),2);%we(sum of epsilon,count of epsilon);
    for i=52:length(weeklist)
        for j=1:length(monthlist)
            if weeklist(i,1)==monthlist(j,1)&&weeklist(i,3)==monthlist(j,2)
                wb(j,:,1)=wb(j,:,1)+betaweekly(i,:);
                wb(j,:,2)=wb(j,:,2)+1;
                wa(j,:,1)=wa(j,:,1)+alphaweekly(i,:);
                wa(j,:,2)=wa(j,:,2)+1;
                ws(j,:,1)=ws(j,:,1)+sigmaweekly(i,:);
                ws(j,:,2)=ws(j,:,2)+1;
                we(j,:,1)=we(j,:,1)+epsilonweekly(i,:);
                we(j,:,2)=we(j,:,2)+1;
            end
        end
    end
    for i=1:length(monthlist)
        for j=1:length(validstock)
            if wb(i,j,2)>0
                avgweeklybetamontly(i,j)=wb(i,j,1)./wb(i,j,2);
            end
            if wa(i,j,2)>0
                avgweeklyalphamontly(i,j)=wa(i,j,1)./wa(i,j,2);
            end
            if ws(i,j,2)>0
                avgweeklysigmamontly(i,j)=ws(i,j,1)./ws(i,j,2);
            end
            if we(i,j,2)>0
                avgweeklyepsilonmontly(i,j)=we(i,j,1)./we(i,j,2);
            end
        end
    end
    
    %取总股本、流通股本 datamonthly第9、10张表
    for i=1:length(validstock)
        %获得股本变动表
        sqlstr=['SELECT F_START_DATE, F2, F4 FROM stock.T_COM_SHARE WHERE (F_STOCK_CODE = ',validstock(i,:), ') ORDER BY F_START_DATE'];
        curs=exec(conn2,sqlstr);
        curs=fetch(curs);
        data=curs.Data;
        dt=cell2mat(data(:,1));
        p=cell2mat(data(:,2:3));
        sharedatelist=datenum(fix(dt/10000),fix(mod(dt,10000)/100),mod(dt,100));
        [Y, M, D, H, MN, S] = datevec(sharedatelist);
        for j=1:length(Y)
            for k=length(monthlist):-1:1
                if  Y(j)*100+M(j)<=monthlist(k,1)*100+monthlist(k,2)
                   datamonthly(k,i,9)=p(j,1);
                   datamonthly(k,i,10)=p(j,2);
                end
            end
        end
    end
    close(curs);      %关闭游标对象 
           
    %从T_BALANCE_STD07，资产表中取数据
    %取总资产 datamonthly第11张表，所有者权益第14张表
    for i=1:length(validstock)
        %获得总资产表（F1），营业利润（F4），所有者权益（F2）
        sqlstr=['SELECT F_END_DATE, F1000,F2400 FROM stock.T_BALANCE_STD07 WHERE (F_STOCK_CODE = ',validstock(i,:), ')  AND F_REPORT_NEW=1 AND F_IS_MERGE=1 ORDER BY F_END_DATE'];
        curs=exec(conn2,sqlstr);
        curs=fetch(curs);
        data=curs.Data;
        dt=cell2mat(data(:,1));
        p=cell2mat(data(:,2:3));
        sharedatelist=datenum(fix(dt/10000),fix(mod(dt,10000)/100),mod(dt,100));
        [Y, M, D, H, MN, S] = datevec(sharedatelist);
        for j=1:length(Y)
            if M(j)==12
                %年报
                for k=1:length(monthlist)
                    if monthlist(k,1)==Y(j)+1 && monthlist(k,2)>=5 && monthlist(k,2)<=8
                        datamonthly(k,i,11)=p(j,1);
                        datamonthly(k,i,14)=p(j,2);
                    end
                end
            elseif M(j)==6
                %中报
                for k=1:length(monthlist)
                    if monthlist(k,1)==Y(j) && monthlist(k,2)>=9 && monthlist(k,2)<=10
                        datamonthly(k,i,11)=p(j,1);
                        datamonthly(k,i,14)=p(j,2);
                    end
                end
            elseif M(j)==9
                %三季报
                for k=1:length(monthlist)
                    if (monthlist(k,1)==Y(j) && monthlist(k,2)>=11 && monthlist(k,2)<=12) || (monthlist(k,1)==Y(j)+1 && monthlist(k,2)>=1 && monthlist(k,2)<=4)
                        datamonthly(k,i,11)=p(j,1);
                        datamonthly(k,i,14)=p(j,2);
                    end
                end
            end
        end
    end
    close(curs);      %关闭游标对象
    
    %从T_PROFIT_STD07，利润表中取数据
    %营业利润 第12张表，营业收入 datamonthly第13张表
    for i=1:length(validstock)
        %获得营业利润（F0200），营业收入表（F0100）
        sqlstr=['SELECT F_END_DATE, F0200, F0100 FROM stock.T_PROFIT_STD07 WHERE (F_STOCK_CODE = ',validstock(i,:), ') AND F_REPORT_NEW=1 AND F_IS_MERGE=1 ORDER BY F_END_DATE'];
        curs=exec(conn2,sqlstr);
        curs=fetch(curs);
        data=curs.Data;
        dt=cell2mat(data(:,1));
        p=cell2mat(data(:,2:3));
        sharedatelist=datenum(fix(dt/10000),fix(mod(dt,10000)/100),mod(dt,100));
        [Y, M, D, H, MN, S] = datevec(sharedatelist);
        for j=1:length(Y)
            if M(j)==12
                %年报
                for k=1:length(monthlist)
                    if monthlist(k,1)==Y(j)+1 && monthlist(k,2)>=5 && monthlist(k,2)<=8
                        datamonthly(k,i,12)=p(j,1);
                        datamonthly(k,i,13)=p(j,2);
                    end
                end
            elseif M(j)==6
                %中报
                for k=1:length(monthlist)
                    if monthlist(k,1)==Y(j) && monthlist(k,2)>=9 && monthlist(k,2)<=10
                        datamonthly(k,i,12)=p(j,1);
                        datamonthly(k,i,13)=p(j,2);
                    end
                end
            elseif M(j)==9
                %三季报
                for k=1:length(monthlist)
                    if (monthlist(k,1)==Y(j) && monthlist(k,2)>=11 && monthlist(k,2)<=12) || (monthlist(k,1)==Y(j)+1 && monthlist(k,2)>=1 && monthlist(k,2)<=4)
                        datamonthly(k,i,12)=p(j,1);
                        datamonthly(k,i,13)=p(j,2);
                    end
                end
            end
        end
    end
    close(curs);      %关闭游标对象
    close(conn);      %关闭数据库连接对象 
    close(conn2);      %关闭数据库连接对象 

    %因素计算
    %因素1 月beta
    F(:,:,1)=betamonthly;
    %因素2 周beta
    F(:,:,2)=avgweeklybetamontly;
    %因素3 总市值（对数）
    F(:,:,3)=log(datamonthly(:,:,9).*datamonthly(:,:,5));
    %因素4 总流通市值（对数）
    F(:,:,4)=log(datamonthly(:,:,10).*datamonthly(:,:,5));
    %因素5 总资产（对数）
    F(:,:,5)=log(datamonthly(:,:,11));
    %因素6 营业利润/总市值
    F(:,:,6)=datamonthly(:,:,12)./(datamonthly(:,:,9).*datamonthly(:,:,5));
    %因素7 营业收入/总市值
    F(:,:,7)=datamonthly(:,:,13)./(datamonthly(:,:,9).*datamonthly(:,:,5));
    %因素8 所有者权益/总市值
    F(:,:,8)=datamonthly(:,:,14)./(datamonthly(:,:,9).*datamonthly(:,:,5));
    %因素9 买卖循环率
    F(:,:,9)=datamonthly(:,:,7)./(datamonthly(:,:,10).*datamonthly(:,:,5));
    %因素10 每日交易额的变动性
    F(:,:,10)=datamonthly(:,:,15);
    %因素11 买卖资金的变化（25日/120日）
    F(:,:,11)=datamonthly(:,:,16);
    %因素12 买卖资金的变化（75日/250日）
    F(:,:,12)=datamonthly(:,:,17);
    %因素13 股价变动的平均偏离（25日）
    F(:,:,13)=datamonthly(:,:,18);
    %因素14 股价变动的平均偏离（75日）
    F(:,:,14)=datamonthly(:,:,19);
    %求日平均
    for i=1:length(monthlist)
        F(i,:,9)=F(i,:,9)./monthlist(i,3);
        F(i,:,10)=F(i,:,10)./monthlist(i,3);
        F(i,:,11)=F(i,:,11)./monthlist(i,3);
        F(i,:,12)=F(i,:,12)./monthlist(i,3);
        F(i,:,13)=F(i,:,13)./monthlist(i,3);
        F(i,:,14)=F(i,:,14)./monthlist(i,3);
    end
    %因素15 月alpha
    F(:,:,15)=alphamonthly;
    %因素16 周alpha
    F(:,:,16)=avgweeklyalphamontly;
    %因素17 月标准差
    F(:,:,17)=sigmamonthly;
    %因素18 周标准差
    F(:,:,18)=avgweeklysigmamontly;
    %因素19 月残差
    F(:,:,19)=epsilonmonthly;
    %因素20 周残差
    F(:,:,20)=avgweeklyepsilonmontly;
    %因素21 负债比率（账面价值）
    F(:,:,21)=(datamonthly(:,:,11)-datamonthly(:,:,14))./datamonthly(:,:,14);
    %因素22 负债比率（盯市价值）
    F(:,:,22)=(datamonthly(:,:,11)-datamonthly(:,:,14))./(datamonthly(:,:,9).*datamonthly(:,:,5));
    %因素23 营业收入营业利润率
    F(:,:,23)=datamonthly(:,:,12)./datamonthly(:,:,13);
    %因素24 总资产营业利润率
    F(:,:,24)=datamonthly(:,:,12)./datamonthly(:,:,11);
    %因素25 营业收入增长速度
    %因素26 总资产增长速度
    %因素27 营业收入营业利润率TREND
    %因素28 总资产营业利润率TREND
    F(:,:,25)=zeros(length(monthlist),length(validstock));
    F(:,:,26)=zeros(length(monthlist),length(validstock));
    F(:,:,27)=zeros(length(monthlist),length(validstock));
    F(:,:,28)=zeros(length(monthlist),length(validstock));
    for i=24:length(monthlist)
        XX=ones(8,2);
        XX(:,2)=[1:8]';
        n=1;
        for j=i-21:3:i
            YR(n,:)=datamonthly(j,:,13);
            YA(n,:)=datamonthly(j,:,11);
            YRP(n,:)=datamonthly(j,:,12)./datamonthly(j,:,13);
            YAP(n,:)=datamonthly(j,:,12)./datamonthly(j,:,11);
            n=n+1;
        end
        for j=1:length(validstock)
            [b,se_b,mse] = lscov(XX,YR(:,j));
            F(i,j,25)=b(2)/mean(YR(:,j));
            [b,se_b,mse] = lscov(XX,YA(:,j));
            F(i,j,26)=b(2)/mean(YA(:,j));
            [b,se_b,mse] = lscov(XX,YRP(:,j));
            F(i,j,27)=b(2);
            [b,se_b,mse] = lscov(XX,YAP(:,j));
            F(i,j,28)=b(2);
        end
    end
    
    
    %从第47个月开始去极值和标准化
    FD=F;
    FS=F;
    for i=47:length(monthlist)
        for j=1:28
            Dm=median(F(i-23:i,:,j));
            Dmad=median(abs(F(i-23:i,:,j)-repmat(Dm,24,1)));
            Dupper=Dm+5.2*Dmad;
            Dlower=Dm-5.2*Dmad;
            for k=i-23:i
                for m=1:length(validstock)
                    if F(k,m,j)>=Dupper(1,m);
                        FD(k,m,j)=Dupper(1,m);
                    elseif F(k,m,j)<=Dlower(1,m)
                        FD(k,m,j)=Dlower(1,m);
                    end
                end
            end
            FS(i,:,j)=(FD(i,:,j)-mean(FD(i-23:i,:,j)))./std(FD(i-23:i,:,j));
        end
    end
    
    %等权重降维
    FF(:,:,1)=(FS(:,:,1)+FS(:,:,2))./2;
    FF(:,:,2)=(FS(:,:,3)+FS(:,:,4)+FS(:,:,5))./3;
    FF(:,:,3)=(FS(:,:,6)+FS(:,:,7)+FS(:,:,8))./3;
    FF(:,:,4)=(FS(:,:,9)+FS(:,:,10)+FS(:,:,11)+FS(:,:,12))./4;
    FF(:,:,5)=(FS(:,:,13)+FS(:,:,14)+FS(:,:,15)+FS(:,:,16)+FS(:,:,7)+FS(:,:,18)+FS(:,:,19)+FS(:,:,20))./8;
    FF(:,:,6)=(FS(:,:,21)+FS(:,:,22))./2;
    FF(:,:,7)=(FS(:,:,23)+FS(:,:,24)+FS(:,:,25)+FS(:,:,26)+FS(:,:,27)+FS(:,:,28))./6;
    
    Factors=zeros(length(validstock),7,length(monthlist));
    %全因素面板数据
    for i=1:length(monthlist)
        for j=1:7
            Factors(:,j,i)=FF(i,:,j)';
        end
    end
    
    %截面回归
    for i=47:length(monthlist)
        XCS=ones(length(validstock),8);
        XCS(:,2:end)=Factors(:,:,i);
        XCS(find(isnan(XCS)==1))=0;
        YCS=datamonthly(i,:,8)';
        YCS(find(isnan(YCS)==1))=0;
        [b,se_b,mse] = lscov(XCS,YCS);
        betacs(i,:)=b';
        %计算各股风险调整后收益率
        returncs(i,:)=betacs(i,:)*XCS';
        %排序
        [B,IX]=sort(returncs(i,:),'descend');
        %构建组合
        portfolio(i,:)=IX;
    end
    
    %计算累积收益率
    for i=48:length(monthlist)
        portfolioreturn(i)=0;
        for j=1:30
            portfolioreturn(i)=portfolioreturn(i)+datamonthly(i,portfolio(i-1,j),8);
        end
        portfolioreturn(i)=portfolioreturn(i)/30;
        %沪深300指数收益率
        idxreturn(i)=idxmonthly(i,2);
    end
    
    portfoliocumreturn=cumsum(portfolioreturn);
    idxcumreturn=cumsum(idxreturn);
    
    plot(47:length(monthlist),portfoliocumreturn(47:end),47:length(monthlist),idxcumreturn(47:end));
    legend('Portfolio','HS300');
    toc
end
