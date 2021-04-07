import MySQLdb


def cash_flow_report():
    
    mycursor = mydb.cursor()
    mycursor.execute("truncate table dashboard_cash_flow_report;")
    mycursor.close()
    
    mycursor = mydb.cursor()
    mycursor.execute("SELECT Distinct coa_mapping FROM v_master_trx where coa_mapping like '%Cash-in-hand->Current Assets%' \
                     union \
                     SELECT Distinct coa_mapping FROM v_master_trx where coa_mapping like '%Bank Accounts->Current Assets%';")
    myresult = mycursor.fetchall()
    mycursor.close()
    
    finalresult=[]
    for data in myresult:
            finalresult.append(data[0])
    
    res=str(finalresult)[1:-1]
    
    query="""insert ignore into dashboard_cash_flow_report (company, voucher_month, net_fund_inflow, net_fund_outflow, closing_balance)
        	select t.company, t.voucher_month, ifnull(inflow,0)+ifnull(contrainflow,0), ifnull(outflow,0)+ifnull(contraoutflow,0), ifnull(opening_balance,0)+ifnull(inflow,0)+ ifnull(contrainflow,0)-(ifnull(outflow,0)+ifnull(contraoutflow,0)) as closing_balance from
        	(select company,voucher_month from v_master_trx where coa_mapping in ("""+res+""") group by company,voucher_month) t
        	left join
        	(select company,voucher_month,sum(ifnull(amount,0)) inflow from v_master_trx where coa_mapping in ("""+res+""") and voucher_type like '%Receipt%'
        	group by company,voucher_month) t1 on t.company=t1.company and t.voucher_month=t1.voucher_month
        	left join
        	(select company,voucher_month,sum(ifnull(amount,0)) outflow from v_master_trx where coa_mapping in ("""+res+""") and voucher_type like '%Payment%'
        	group by company,voucher_month) t2 on t.company=t2.company and t.voucher_month=t2.voucher_month
        	left join
        	(select company,voucher_month,sum(ifnull(amount,0)) contrainflow from v_master_trx where coa_mapping in ("""+res+""") and voucher_type like '%Contra%' and amount < 0
        	group by company,voucher_month) t3 on t.company=t3.company and t.voucher_month=t3.voucher_month
        	left join
        	(select company,voucher_month,sum(ifnull(amount,0)) contraoutflow from v_master_trx where coa_mapping in ("""+res+""") and voucher_type like '%Contra%' and amount > 0
        	group by company,voucher_month) t4 on t.company=t4.company and t.voucher_month=t4.voucher_month
        	left join
        	(SELECT company, voucher_month, sum(ifnull(opening_balance,0)) opening_balance FROM dashboard_cash_equivalent d group by company,voucher_month) t5
        	on t.company=t5.company and t.voucher_month=t5.voucher_month; """
    mycursor = mydb.cursor()
    mycursor.execute(query)
    mycursor.close()
    mydb.commit()
    

def cash_burn():
    query="""insert ignore into dashboard_cash_burn(company, voucher_month, cash_burn, financial_year)
            SELECT m.company, m.voucher_month, ifnull(p.sale_amount,0)+ifnull(d.sale_amount,0)+ifnull(i.sale_amount,0) cash_burn, m.fyear FROM
            (SELECT distinct company,fyear,voucher_month FROM v_master_trx ) m
            left join dashboard_purchase_report p on m.company=p.company and m.voucher_month=p.voucher_month
            left join dashboard_directexp_report d on m.company=d.company and m.voucher_month=d.voucher_month
            left join dashboard_indirectexp_report i on m.company=i.company and m.voucher_month=i.voucher_month; """
    mycursor = mydb.cursor()
    mycursor.execute(query)
    mycursor.close()
    mydb.commit()


def update_average():
    mycursor = mydb.cursor()
    mycursor.execute("Select distinct company from dashboard_cash_burn")
    myresult = mycursor.fetchall()
    mycursor.close()
    
    for data in myresult:
        
        mycursor = mydb.cursor()
        query="SELECT id, avg_expense, cash_burn FROM dashboard_cash_burn d where company=%s order by str_to_date(voucher_month,'%%M-%%y');"
        mycursor.execute(query,[data[0]])
        myresult = mycursor.fetchall()
        mycursor.close()
        
#        query="select id, voucher_month, cash_burn, avg_expense from dashboard_cash_burn where financial_year=%s and company=%s order by str_to_date(voucher_month,'%%M-%%y');"
#        mycursor = mydb.cursor()
#        datatuple=(year,company)
#        mycursor.execute(query, datatuple)
#        myresult1 = mycursor.fetchall()
#        mycursor.close()
        
        myresult = [list(row) for row in myresult]
        
        params=[]
        summation=[]
        cash_burn=[]
        for count, data1 in enumerate(myresult,start=0):
            cash_burn.append(float(data1[2]))
            if count==0:
#                if data1[3]==None:
                    data1[1]=data1[2]
                    params.append([data1[1],data1[0]])
                    summation.append(data1[1])
                
            else:
#                if data1[3]==None:
                if count <=11:
                    add= float(summation[count-1]) + float(data1[2])
                    data1[1]= add/(count+1)
                    params.append([data1[1],data1[0]])
                    summation.append(add)
                if count > 11:
                    lst=sum(cash_burn[-12:])/12
                    params.append([str(lst),data1[0]])
        
        mycursor = mydb.cursor()
        query1= "update dashboard_cash_burn set avg_expense=%s where id=%s;"
        mycursor.executemany(query1,params)
        mycursor.close()
        mydb.commit()
        
def update_cashrunway():
    
    query="""update dashboard_cash_burn b,
            (select t.company, t.voucher_month, (t.transaction_amount+t.opening_balance)/p.avg_expense as cash_run_way from
            (SELECT company, voucher_month, transaction_amount, opening_balance FROM dashboard_cash_equivalent d group by company, voucher_month) t
            join dashboard_cash_burn p on p.company=t.company and p.voucher_month=t.voucher_month) tm set b.cash_run_way=tm.cash_run_way where tm.company=b.company and tm.voucher_month=b.voucher_month;"""
    mycursor = mydb.cursor()
    mycursor.execute(query)
    mycursor.close()

    mydb.commit()


cash_flow_report()
#cash_burn()
#update_average()
#update_cashrunway()
print('finished')
mydb.close()
            
            
            
            
            
            
    