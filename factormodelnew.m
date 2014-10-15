clear;close all
%url
databaseurl='jdbc:sqlserver://202.121.129.201:1433;';
databaseurl2='jdbc:sqlserver://202.121.129.201:1433;database=txnfdb;';
%driver
driver='com.microsoft.sqlserver.jdbc.SQLServerDriver'; 
username='dbviewer';   %��¼��
password='sufefinlab';   %����
databasename='JRTZ_ANA'; %����Դ����
databasename2='txnfdb'; %����Դ����2
conn = database(databasename,username,password,driver,databaseurl);
conn2 = database(databasename2,username,password,driver,databaseurl2);
%conn=database('JRTZ_ANA','dbviewer','sufefinlab','com.microsoft.sqlserver.jdbc.SQLServerDriver','jdbc:sqlserver://202.121.129.201:1433;database=JRTZ_ANA');

if isconnection(conn)==1 && isconnection(conn2)==1
    tic
    
    %ȡ����
    %QOT_D_BCK��Ϊ��Ȩ�����������ǰ��Ȩ��
    %��Ҫ��������ݣ�����ָ�����������룬��������Ҫ�����ֵ����
    %curs=exec(conn,'select IDX.PUB_DT,A.F0050 from (select * from QOT_D_BCK where SEC_CD=''600000'' and VAR_CL=''A'') A right join (select * from QOT_D_BCK where SEC_CD=''000001'' and VAR_CL=''Z'') IDX on A.PUB_DT=IDX.PUB_DT order by IDX.PUB_DT');
    %����Ҫ��������ݣ�Ŀǰʹ�õ�JRTZ_ANA���ݿ��Ѿ����룬ͣ���ռ۸������һ�����̼ۣ��ɽ���Ϊ0
    %curs=exec(conn,'select PUB_DT,F0050 from QOT_D_BCK where SEC_CD=''600000'' and VAR_CL=''A'' and PUB_DT>=''2010-1-1'' order by PUB_DT');

    %�ӵ�ǰĿ¼��ȡ�����hs300.csv����cell��
    [bankname,bankticker]=textread('hs300.csv','%s%s','delimiter', ',');
    %��cell�еĴ���ȡ����
    banks=cell2mat(bankticker(2:end));

    %��ȡ��ָ֤����ȷ�����ݳ���
    %startdate='2009-4-1';%��ʼ����
    %sqlstr=['select PUB_DT,F0050 from QOT_D_BCK where SEC_CD=''','000300',''' and VAR_CL=''Z'' and PUB_DT>=''',startdate,''' order by PUB_DT'];
    startdate='20050408';%��ʼ����
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
        %���ݲ���Ĺ�Ʊ����
        if length(p)~=length(idx)
            continue
        end
        %����ͣ�Ƴ���60��Ĺ�Ʊ����
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
    close(curs);      %�ر��α���� 
    
    %120��ƽ�����׶��׼�datadaily 8#��
    %25��ƽ�����׶datadaily 9#��
    %75��ƽ�����׶datadaily 10#��
    %120��ƽ�����׶datadaily 11#��
    %250��ƽ�����׶datadaily 12#��
    %25��ƽ�����̼ۣ�datadaily 13#��
    %75��ƽ�����̼ۣ�datadaily 14#��
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
    
    %������ת������
    %monthlistΪ�¶��б�
    monthlist=[2005,4,0];%[��,��,ÿ������]
    n=1;% monthlist(n)Ŀǰ������¶�
    [Y, M, D, H, MN, S] = datevec(datelist);
    datamonthly(1,:,:)=datadaily(1,:,:);
    datamonthly(:,:,8:19)=0;
    idxmonthly(1,:)=idx(1);
    for i=2:length(datelist)
        if Y(i)~=monthlist(n,1) || M(i)~=monthlist(n,2)
            %���·�
            n=n+1;
            monthlist(n,:)=[Y(i), M(i),1];
            %���³�ֵ
            datamonthly(n,:,1: 7)=datadaily(i,:,1:7);
            idxmonthly(n,:)=idx(i);
        else
            for j=1:size(datadaily,2)
                %����߼�
                if datadaily(i,j,3)>datamonthly(n,j,3)
                    datamonthly(n,j,3)=datadaily(i,j,3);
                end
                %����ͼ�
                if datadaily(i,j,4)<datamonthly(n,j,4)
                    datamonthly(n,j,4)=datadaily(i,j,4);
                end
                %�����¼�
                datamonthly(n,j,5)=datadaily(i,j,5);
                %�ɽ���
                datamonthly(n,j,6)=datamonthly(n,j,6)+datadaily(i,j,6);
                %�ɽ����
                datamonthly(n,j,7)=datamonthly(n,j,7)+datadaily(i,j,7);
                %120��ƽ�����׶��׼��ĺ� datamonthly 15#
                datamonthly(n,j,15)=datamonthly(n,j,15)+datadaily(i,j,8);
                %25��/120��ƽ�����׶�ĺ� datamonthly 16#
                if datadaily(i,j,11)~=0
                    datamonthly(n,j,16)=datamonthly(n,j,16)+datadaily(i,j,9)./datadaily(i,j,11);
                end
                %75��/250��ƽ�����׶�ĺ� datamonthly 17#
                if datadaily(i,j,12)~=0
                    datamonthly(n,j,17)=datamonthly(n,j,17)+datadaily(i,j,10)./datadaily(i,j,12);
                end
                %��25�ձ䶯ƽ���ɼ�-ǰһ��ɼۣ�/25�ձ䶯ƽ���ɼ۵ĺ� 18#
                if datadaily(i,j,9)~=0
                    datamonthly(n,j,18)=datamonthly(n,j,18)+(datadaily(i,j,13)-datadaily(i-1,j,5))./datadaily(i,j,13);
                end
                %��75�ձ䶯ƽ���ɼ�-ǰһ��ɼۣ�/75�ձ䶯ƽ���ɼ۵ĺ� 19#
                if datadaily(i,j,10)~=0
                    datamonthly(n,j,19)=datamonthly(n,j,19)+(datadaily(i,j,14)-datadaily(i-1,j,5))./datadaily(i,j,14);
                end
            end
            idxmonthly(n,:)=idx(i);
            monthlist(n,3)=monthlist(n,3)+1;
        end
    end
   
    %����ع�ϵ
    covmat=cov(datamonthly(:,:,5));%Э�������
    corrmat=corrcoef(datamonthly(:,:,5));%���ϵ������

    %�¶��������� datamonthly��8�ű�
    datamonthly(2:end,:,8)=diff(log(datamonthly(:,:,5)));
    idxmonthly(2:end,2)=diff(log(idxmonthly(:,1)));

    %����1
    %����beta 24�����ƶ�����
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
    
    %������ת������
    %weeklistΪ�ܶ��б�
    weeklist=[2005,1,4];%[Y,W,M]
    n=1;% weeklist(n)Ŀǰ������ܶ�
    [Y, M, D, H, MN, S] = datevec(datelist);
    dataweekly(1,:,:)=datadaily(1,:,1:7);
    idxweekly(1,:)=idx(1);
    W=fix((datelist-datenum('2005-4-4')+1)/7+1);%����
    for i=2:length(datelist)
        if  W(i)~=weeklist(n,2)
            %����
            n=n+1;
            weeklist(n,:)=[Y(i), W(i),M(i)];
            %���ܳ�ֵ
            dataweekly(n,:,1:7)=datadaily(i,:,1:7);
            idxweekly(n,:)=idx(i);
        else
            for j=1:size(datadaily,2)
                %����߼�
                if datadaily(i,j,3)>dataweekly(n,j,3)
                    dataweekly(n,j,3)=datadaily(i,j,3);
                end
                %����ͼ�
                if datadaily(i,j,4)<dataweekly(n,j,4)
                    dataweekly(n,j,4)=datadaily(i,j,4);
                end
                %�����¼�
                dataweekly(n,j,5)=datadaily(i,j,5);
                %�ɽ���
                dataweekly(n,j,6)=dataweekly(n,j,6)+datadaily(i,j,6);
                %�ɽ����
                dataweekly(n,j,7)=dataweekly(n,j,7)+datadaily(i,j,7);
            end
            idxweekly(n,:)=idx(i);
        end
    end    
   
    %�ܶ��������� dataweekly��8�ű�
    dataweekly(2:end,:,8)=diff(log(dataweekly(:,:,5)));
    idxweekly(2:end,2)=diff(log(idxweekly(:,1)));

    %����2
    %����beta 52���ƶ�����
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
    %���¶���ƽ��beta��alpha����׼��в�
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
    
    %ȡ�ܹɱ�����ͨ�ɱ� datamonthly��9��10�ű�
    for i=1:length(validstock)
        %��ùɱ��䶯��
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
    close(curs);      %�ر��α���� 
           
    %��T_BALANCE_STD07���ʲ�����ȡ����
    %ȡ���ʲ� datamonthly��11�ű�������Ȩ���14�ű�
    for i=1:length(validstock)
        %������ʲ���F1����Ӫҵ����F4����������Ȩ�棨F2��
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
                %�걨
                for k=1:length(monthlist)
                    if monthlist(k,1)==Y(j)+1 && monthlist(k,2)>=5 && monthlist(k,2)<=8
                        datamonthly(k,i,11)=p(j,1);
                        datamonthly(k,i,14)=p(j,2);
                    end
                end
            elseif M(j)==6
                %�б�
                for k=1:length(monthlist)
                    if monthlist(k,1)==Y(j) && monthlist(k,2)>=9 && monthlist(k,2)<=10
                        datamonthly(k,i,11)=p(j,1);
                        datamonthly(k,i,14)=p(j,2);
                    end
                end
            elseif M(j)==9
                %������
                for k=1:length(monthlist)
                    if (monthlist(k,1)==Y(j) && monthlist(k,2)>=11 && monthlist(k,2)<=12) || (monthlist(k,1)==Y(j)+1 && monthlist(k,2)>=1 && monthlist(k,2)<=4)
                        datamonthly(k,i,11)=p(j,1);
                        datamonthly(k,i,14)=p(j,2);
                    end
                end
            end
        end
    end
    close(curs);      %�ر��α����
    
    %��T_PROFIT_STD07���������ȡ����
    %Ӫҵ���� ��12�ű�Ӫҵ���� datamonthly��13�ű�
    for i=1:length(validstock)
        %���Ӫҵ����F0200����Ӫҵ�����F0100��
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
                %�걨
                for k=1:length(monthlist)
                    if monthlist(k,1)==Y(j)+1 && monthlist(k,2)>=5 && monthlist(k,2)<=8
                        datamonthly(k,i,12)=p(j,1);
                        datamonthly(k,i,13)=p(j,2);
                    end
                end
            elseif M(j)==6
                %�б�
                for k=1:length(monthlist)
                    if monthlist(k,1)==Y(j) && monthlist(k,2)>=9 && monthlist(k,2)<=10
                        datamonthly(k,i,12)=p(j,1);
                        datamonthly(k,i,13)=p(j,2);
                    end
                end
            elseif M(j)==9
                %������
                for k=1:length(monthlist)
                    if (monthlist(k,1)==Y(j) && monthlist(k,2)>=11 && monthlist(k,2)<=12) || (monthlist(k,1)==Y(j)+1 && monthlist(k,2)>=1 && monthlist(k,2)<=4)
                        datamonthly(k,i,12)=p(j,1);
                        datamonthly(k,i,13)=p(j,2);
                    end
                end
            end
        end
    end
    close(curs);      %�ر��α����
    close(conn);      %�ر����ݿ����Ӷ��� 
    close(conn2);      %�ر����ݿ����Ӷ��� 

    %���ؼ���
    %����1 ��beta
    F(:,:,1)=betamonthly;
    %����2 ��beta
    F(:,:,2)=avgweeklybetamontly;
    %����3 ����ֵ��������
    F(:,:,3)=log(datamonthly(:,:,9).*datamonthly(:,:,5));
    %����4 ����ͨ��ֵ��������
    F(:,:,4)=log(datamonthly(:,:,10).*datamonthly(:,:,5));
    %����5 ���ʲ���������
    F(:,:,5)=log(datamonthly(:,:,11));
    %����6 Ӫҵ����/����ֵ
    F(:,:,6)=datamonthly(:,:,12)./(datamonthly(:,:,9).*datamonthly(:,:,5));
    %����7 Ӫҵ����/����ֵ
    F(:,:,7)=datamonthly(:,:,13)./(datamonthly(:,:,9).*datamonthly(:,:,5));
    %����8 ������Ȩ��/����ֵ
    F(:,:,8)=datamonthly(:,:,14)./(datamonthly(:,:,9).*datamonthly(:,:,5));
    %����9 ����ѭ����
    F(:,:,9)=datamonthly(:,:,7)./(datamonthly(:,:,10).*datamonthly(:,:,5));
    %����10 ÿ�ս��׶�ı䶯��
    F(:,:,10)=datamonthly(:,:,15);
    %����11 �����ʽ�ı仯��25��/120�գ�
    F(:,:,11)=datamonthly(:,:,16);
    %����12 �����ʽ�ı仯��75��/250�գ�
    F(:,:,12)=datamonthly(:,:,17);
    %����13 �ɼ۱䶯��ƽ��ƫ�루25�գ�
    F(:,:,13)=datamonthly(:,:,18);
    %����14 �ɼ۱䶯��ƽ��ƫ�루75�գ�
    F(:,:,14)=datamonthly(:,:,19);
    %����ƽ��
    for i=1:length(monthlist)
        F(i,:,9)=F(i,:,9)./monthlist(i,3);
        F(i,:,10)=F(i,:,10)./monthlist(i,3);
        F(i,:,11)=F(i,:,11)./monthlist(i,3);
        F(i,:,12)=F(i,:,12)./monthlist(i,3);
        F(i,:,13)=F(i,:,13)./monthlist(i,3);
        F(i,:,14)=F(i,:,14)./monthlist(i,3);
    end
    %����15 ��alpha
    F(:,:,15)=alphamonthly;
    %����16 ��alpha
    F(:,:,16)=avgweeklyalphamontly;
    %����17 �±�׼��
    F(:,:,17)=sigmamonthly;
    %����18 �ܱ�׼��
    F(:,:,18)=avgweeklysigmamontly;
    %����19 �²в�
    F(:,:,19)=epsilonmonthly;
    %����20 �ܲв�
    F(:,:,20)=avgweeklyepsilonmontly;
    %����21 ��ծ���ʣ������ֵ��
    F(:,:,21)=(datamonthly(:,:,11)-datamonthly(:,:,14))./datamonthly(:,:,14);
    %����22 ��ծ���ʣ����м�ֵ��
    F(:,:,22)=(datamonthly(:,:,11)-datamonthly(:,:,14))./(datamonthly(:,:,9).*datamonthly(:,:,5));
    %����23 Ӫҵ����Ӫҵ������
    F(:,:,23)=datamonthly(:,:,12)./datamonthly(:,:,13);
    %����24 ���ʲ�Ӫҵ������
    F(:,:,24)=datamonthly(:,:,12)./datamonthly(:,:,11);
    %����25 Ӫҵ���������ٶ�
    %����26 ���ʲ������ٶ�
    %����27 Ӫҵ����Ӫҵ������TREND
    %����28 ���ʲ�Ӫҵ������TREND
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
    
    
    %�ӵ�47���¿�ʼȥ��ֵ�ͱ�׼��
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
    
    %��Ȩ�ؽ�ά
    FF(:,:,1)=(FS(:,:,1)+FS(:,:,2))./2;
    FF(:,:,2)=(FS(:,:,3)+FS(:,:,4)+FS(:,:,5))./3;
    FF(:,:,3)=(FS(:,:,6)+FS(:,:,7)+FS(:,:,8))./3;
    FF(:,:,4)=(FS(:,:,9)+FS(:,:,10)+FS(:,:,11)+FS(:,:,12))./4;
    FF(:,:,5)=(FS(:,:,13)+FS(:,:,14)+FS(:,:,15)+FS(:,:,16)+FS(:,:,7)+FS(:,:,18)+FS(:,:,19)+FS(:,:,20))./8;
    FF(:,:,6)=(FS(:,:,21)+FS(:,:,22))./2;
    FF(:,:,7)=(FS(:,:,23)+FS(:,:,24)+FS(:,:,25)+FS(:,:,26)+FS(:,:,27)+FS(:,:,28))./6;
    
    Factors=zeros(length(validstock),7,length(monthlist));
    %ȫ�����������
    for i=1:length(monthlist)
        for j=1:7
            Factors(:,j,i)=FF(i,:,j)';
        end
    end
    
    %����ع�
    for i=47:length(monthlist)
        XCS=ones(length(validstock),8);
        XCS(:,2:end)=Factors(:,:,i);
        XCS(find(isnan(XCS)==1))=0;
        YCS=datamonthly(i,:,8)';
        YCS(find(isnan(YCS)==1))=0;
        [b,se_b,mse] = lscov(XCS,YCS);
        betacs(i,:)=b';
        %������ɷ��յ�����������
        returncs(i,:)=betacs(i,:)*XCS';
        %����
        [B,IX]=sort(returncs(i,:),'descend');
        %�������
        portfolio(i,:)=IX;
    end
    
    %�����ۻ�������
    for i=48:length(monthlist)
        portfolioreturn(i)=0;
        for j=1:30
            portfolioreturn(i)=portfolioreturn(i)+datamonthly(i,portfolio(i-1,j),8);
        end
        portfolioreturn(i)=portfolioreturn(i)/30;
        %����300ָ��������
        idxreturn(i)=idxmonthly(i,2);
    end
    
    portfoliocumreturn=cumsum(portfolioreturn);
    idxcumreturn=cumsum(idxreturn);
    
    plot(47:length(monthlist),portfoliocumreturn(47:end),47:length(monthlist),idxcumreturn(47:end));
    legend('Portfolio','HS300');
    toc
end
