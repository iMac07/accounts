package org.xersys.accounts.client;

import com.sun.rowset.CachedRowSetImpl;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;
import javax.sql.RowSetMetaData;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;
import org.xersys.commander.contants.EditMode;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.util.MiscUtil;
import org.xersys.commander.util.SQLUtil;

public class APClientTrans {
    private static final String MASTER_TABLE = "AP_Master";
    private static final String DETAIL_TABLE = "AP_Ledger";
    
    private final XNautilus p_oNautilus;
    private final String p_sBranchCd;
    
    private String p_sMessage;
    
    private Date p_dTransact;
    private String p_sSourceCd;
    private String p_sSourceNo;
    private String p_sClientID;
    private double p_nCreditxx;
    private double p_nDebitxxx;
    private int p_nEditMode;
    
    private double p_nABalance;
    private double p_nCredLimt;
    
    private CachedRowSet p_oDetail;
    
    public APClientTrans(XNautilus foNautilus, String fsBranchCd){
        p_oNautilus = foNautilus;
        p_sBranchCd = fsBranchCd;
        
        createTransaction();
    }
    
    public String getMessage(){
        return p_sMessage;
    }
    
    public boolean AddDetail(String fsSourceCd,
                            String fsSourceNo,
                            Date fdTransact,
                            double fnCreditxx,
                            double fnDebitxxx){
        try {
            p_oDetail.last();
            p_oDetail.moveToInsertRow();

            MiscUtil.initRowSet(p_oDetail);
            p_oDetail.updateObject("sSourceCd", fsSourceCd);
            p_oDetail.updateObject("sSourceNo", fsSourceNo);
            p_oDetail.updateObject("dTransact", fdTransact);
            p_oDetail.updateObject("nCreditxx", fnCreditxx);
            p_oDetail.updateObject("nDebitxxx", fnDebitxxx);

            p_oDetail.insertRow();
            p_oDetail.moveToCurrentRow();
        } catch (SQLException e) {
            e.printStackTrace();
            p_sMessage = e.getMessage();
            return false;
        }
        
        return true;
    }
    
    public boolean Purchase(String fsSourceNo,
                            String fsClientID,
                            Date fdTransact,
                            double fnCreditxx,
                            double fnDebitxxx,
                            int fnEditMode){
        
        p_sSourceCd = "DA";
        p_sSourceNo = fsSourceNo;
        p_sClientID = fsClientID;
        p_dTransact = fdTransact;
        p_nCreditxx = fnCreditxx;
        p_nDebitxxx = fnDebitxxx;
        p_nEditMode = fnEditMode;
        
        return saveTransaction();
    }
    
    public boolean PurchaseReturn(String fsSourceNo,
                                    String fsClientID,
                                    Date fdTransact,
                                    double fnCreditxx,
                                    double fnDebitxxx,
                                    int fnEditMode){
        
        p_sSourceCd = "PR";
        p_sSourceNo = fsSourceNo;
        p_sClientID = fsClientID;
        p_dTransact = fdTransact;
        p_nCreditxx = fnCreditxx;
        p_nDebitxxx = fnDebitxxx;
        p_nEditMode = fnEditMode;
        
        return saveTransaction();
    }
    
    public boolean PaymentIssue(String fsSourceNo,
                                    String fsClientID,
                                    Date fdTransact,
                                    double fnCreditxx,
                                    double fnDebitxxx,
                                    int fnEditMode){
        
        p_sSourceCd = "Py";
        p_sSourceNo = fsSourceNo;
        p_sClientID = fsClientID;
        p_dTransact = fdTransact;
        p_nCreditxx = fnCreditxx;
        p_nDebitxxx = fnDebitxxx;
        p_nEditMode = fnEditMode;
        
        return saveTransaction();
    }
    
    private boolean saveTransaction(){
        try {
            String lsSQL = "SELECT" +
                                "  a.sClientID" +
                                ", a.sBranchCd" +
                                ", a.nCredLimt" +
                                ", a.nABalance" +
                                ", b.sSourceCd" +
                                ", b.sSourceNo" +
                                ", b.nCreditxx" +
                                ", b.nDebitxxx" +
                                ", b.dTransact" +
                            " FROM " + MASTER_TABLE + " a" +
                                " LEFT JOIN " + DETAIL_TABLE + " b" +
                                    " ON a.sClientID = b.sClientID" +
                                        " AND a.sBranchCd = b.sBranchCd" +
                                        " AND b.sSourceCd = " + SQLUtil.toSQL(p_sSourceCd) +
                                        " AND b.sSourceNo = " + SQLUtil.toSQL(p_sSourceNo) +
                            " WHERE a.sClientID = " + SQLUtil.toSQL(p_sClientID) +
                                " AND a.sBranchCd = " + SQLUtil.toSQL(p_sBranchCd);
                        
            ResultSet loRS = p_oNautilus.executeQuery(lsSQL);

            if (!loRS.next()){
                p_sMessage = "Client has NO ACCOUNTS PAYABLE record.";
                return false;
            }
            
            p_nCredLimt = loRS.getDouble("nCredLimt");
            p_nABalance = loRS.getDouble("nABalance");
            
            if (p_nEditMode == EditMode.DELETE){
                if (p_sSourceCd.equals("Py")) 
                    if (!undoPayment()) return false;
                
                return delClientTrans(loRS.getString("sClientID"),
                                        loRS.getDate("dTransact"),
                                        loRS.getString("sSourceCd"),
                                        loRS.getString("sSourceNo"),
                                        loRS.getDouble("nCreditxx"),
                                        loRS.getDouble("nDebitxxx"));
            }
            
            if (p_sSourceCd.equals("Py")) 
                if (!processPayment()) return false;
            
            return addClientTrans();
        } catch (SQLException e) {
            e.printStackTrace();
            p_sMessage = e.getMessage();
            return false;
        }
    }
    
    private boolean processPayment(){
        try {
            p_oDetail.last();
            if (p_oDetail.getRow() > 0) p_oDetail.beforeFirst();
            
            String lsSQL;
            
            while (p_oDetail.next()){
                switch(p_oDetail.getString("sSourceCd")){
                    case "DA":
                        lsSQL = "UPDATE PO_Receiving_Master SET" +
                                    "  nAmtPaidx = nAmtPaidx + " + (p_nDebitxxx - p_nCreditxx) +
                                    ", dModified = " + SQLUtil.toSQL(p_oNautilus.getServerDate()) +
                                " WHERE sTransNox = " + SQLUtil.toSQL(p_oDetail.getString("sSourceNo"));
                        
                        if (p_oNautilus.executeUpdate(lsSQL, "PO_Receiving_Master", p_sBranchCd, "") <= 0){
                            p_sMessage = "Unable to update PO Receiving record.";
                            return false;
                        }
                        break;
                    default:
                        lsSQL = "UPDATE " + DETAIL_TABLE + " SET " +
                                    "  dPostedxx = " + SQLUtil.toSQL(p_dTransact) + 
                                    ", dModified = " + SQLUtil.toSQL(p_oNautilus.getServerDate()) +
                                " WHERE sClientID = " + SQLUtil.toSQL(p_sClientID) +
                                    " AND sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                                    " AND sSourceCd = " + SQLUtil.toSQL(p_oDetail.getString("sSourceCd")) +
                                    " AND sSourceNo = " + SQLUtil.toSQL(p_oDetail.getString("sSourceNo")) +
                                    " AND cReversex = '0'" + 
                                    " AND dPostedxx IS NULL";
                        
                        if (p_oNautilus.executeUpdate(lsSQL, DETAIL_TABLE, p_sBranchCd, "") <= 0){
                            p_sMessage = "Unable to update ACCOUNT LEDGER.";
                            return false;
                        }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            p_sMessage = e.getMessage();
            return false;
        }
        
        return true;
    }
    
    private boolean undoPayment(){
        try {
            p_oDetail.last();
            if (p_oDetail.getRow() > 0) p_oDetail.beforeFirst();
            
            String lsSQL;
            
            while (p_oDetail.next()){
                switch(p_oDetail.getString("sSourceCd")){
                    case "DA":
                        lsSQL = "UPDATE PO_Receiving_Master SET" +
                                    "  nAmtPaidx = nAmtPaidx - " + (p_nDebitxxx - p_nCreditxx) +
                                    ", dModified = " + SQLUtil.toSQL(p_oNautilus.getServerDate()) +
                                " WHERE sTransNox = " + SQLUtil.toSQL(p_oDetail.getString("sSourceNo"));
                        
                        if (p_oNautilus.executeUpdate(lsSQL, "PO_Receiving_Master", p_sBranchCd, "") <= 0){
                            p_sMessage = "Unable to update PO Receiving record.";
                            return false;
                        }
                        break;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            p_sMessage = e.getMessage();
            return false;
        }
        
        return true;
    }
    
    private boolean addClientTrans(){
        String lsSQL = "INSERT INTO " + DETAIL_TABLE + " SET" +
                        "  sClientID = " + SQLUtil.toSQL(p_sClientID) +
                        ", sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                        ", dTransact = " + SQLUtil.toSQL(p_dTransact) +
                        ", sSourceCd = " + SQLUtil.toSQL(p_sSourceCd) +
                        ", sSourceNo = " + SQLUtil.toSQL(p_sSourceNo) +
                        ", cReversex = '0'" +
                        ", nCreditxx = " + p_nCreditxx +
                        ", nDebitxxx = " + p_nDebitxxx +
                        ", dPostedxx = NULL " +
                        ", dModified = " + SQLUtil.toSQL(p_oNautilus.getServerDate()); 
        
        if (p_oNautilus.executeUpdate(lsSQL, DETAIL_TABLE, p_sBranchCd, "") <= 0){
            p_sMessage = "Unable to update CLIENT LEDGER.";
            return false;
        }
        
        if (p_sSourceCd.equals("DA")){
            //recompute account balance
            lsSQL = "UPDATE " + MASTER_TABLE + " SET" +
                        "  nABalance = nABalance + " + (p_nCreditxx - p_nDebitxxx) +
                        ", dModified = " + SQLUtil.toSQL(p_oNautilus.getServerDate()) +
                    " WHERE sClientID = " + SQLUtil.toSQL(p_sClientID) +
                        " AND sBranchCd = " + SQLUtil.toSQL(p_sBranchCd);

            if (p_oNautilus.executeUpdate(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0){
                p_sMessage = "Unable to update CLIENT ACCOUNT.";
                return false;
            }
            
            //post ledger
            lsSQL = "UPDATE " + DETAIL_TABLE + " SET" +
                        "  dPostedxx = " + SQLUtil.toSQL(p_dTransact) +
                        ", dModified = " + SQLUtil.toSQL(p_oNautilus.getServerDate()) +
                    " WHERE sClientID = " + SQLUtil.toSQL(p_sClientID) +
                        " AND sBranchCd = " + SQLUtil.toSQL(p_sBranchCd) +
                        " AND sSourceCd = " + SQLUtil.toSQL(p_sSourceCd) +
                        " AND sSourceNo = " + SQLUtil.toSQL(p_sSourceNo) +
                        " AND cReversex = '0'" +
                        " AND dPostedxx IS NULL";
            
            if (p_oNautilus.executeUpdate(lsSQL, DETAIL_TABLE, p_sBranchCd, "") <= 0){
                p_sMessage = "Unable to update CLIENT LEDGER.";
                return false;
            }
        } else if (p_sSourceCd.equals("PR")){ //*****new
            //recompute account balance
            lsSQL = "UPDATE " + MASTER_TABLE + " SET" +
                        "  nABalance = nABalance + " + (p_nCreditxx - p_nDebitxxx) +
                        ", dModified = " + SQLUtil.toSQL(p_oNautilus.getServerDate()) +
                    " WHERE sClientID = " + SQLUtil.toSQL(p_sClientID) +
                        " AND sBranchCd = " + SQLUtil.toSQL(p_sBranchCd);

            if (p_oNautilus.executeUpdate(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0){
                p_sMessage = "Unable to update CLIENT ACCOUNT.";
                return false;
            }
        }
        
        return true;
    }
    
    private boolean delClientTrans(String fsClientID,
                                    Date fdTransact,
                                    String fsSourceCd,
                                    String fsSourceNo,
                                    double fnCreditxx,
                                    double fnDebitxxx){
        
        String lsSQL = "INSERT INTO " + DETAIL_TABLE + " SET" +
                        "  sClientID = " + SQLUtil.toSQL(fsClientID) +
                        ", sBranchCd = " + SQLUtil.toSQL(fdTransact) +
                        ", dTransact = " + SQLUtil.toSQL(fdTransact) +
                        ", sSourceCd = " + SQLUtil.toSQL(fsSourceCd) +
                        ", sSourceNo = " + SQLUtil.toSQL(fsSourceNo) +
                        ", cReversex = '1'" +
                        ", nCreditxx = " + fnCreditxx +
                        ", nDebitxxx = " + fnDebitxxx +
                        ", dPostedxx = " + SQLUtil.toSQL(p_dTransact) +
                        ", dModified = " + SQLUtil.toSQL(p_oNautilus.getServerDate()); 
        
        if (p_oNautilus.executeUpdate(lsSQL, DETAIL_TABLE, p_sBranchCd, "") <= 0){
            p_sMessage = "Unable to update CLIENT LEDGER.";
            return false;
        }
        
        if (p_sSourceCd.equals("DA") || p_sSourceCd.equals("PR")){
            //recompute account balance
            lsSQL = "UPDATE " + MASTER_TABLE + " SET" +
                        "  nABalance = nABalance - " + (p_nCreditxx - p_nDebitxxx) +
                        ", dModified = " + SQLUtil.toSQL(p_oNautilus.getServerDate()) +
                    " WHERE sClientID = " + SQLUtil.toSQL(p_sClientID) +
                        " AND sBranchCd = " + SQLUtil.toSQL(p_sBranchCd);

            if (p_oNautilus.executeUpdate(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0){
                p_sMessage = "Unable to update CLIENT ACCOUNT.";
                return false;
            }
        }
        
        return true;
    }
    
    private boolean createTransaction(){
        try {
            RowSetMetaData meta = new RowSetMetaDataImpl();

            meta.setColumnCount(5);
            
            meta.setColumnName(1, "sSourceCd");
            meta.setColumnLabel(1, "sSourceCd");
            meta.setColumnType(1, Types.VARCHAR);
            meta.setColumnDisplaySize(1, 4);

            meta.setColumnName(2, "sSourceNo");
            meta.setColumnLabel(2, "sSourceNo");
            meta.setColumnType(2, Types.VARCHAR);
            meta.setColumnDisplaySize(2, 12);

            meta.setColumnName(3, "dTransact");
            meta.setColumnLabel(3, "dTransact");
            meta.setColumnType(3, Types.DATE);

            meta.setColumnName(4, "nCreditxx");
            meta.setColumnLabel(4, "nCreditxx");
            meta.setColumnType(4, Types.DECIMAL);

            meta.setColumnName(5, "nDebitxxx");
            meta.setColumnLabel(5, "nDebitxxx");
            meta.setColumnType(5, Types.DECIMAL);

            p_oDetail = new CachedRowSetImpl();
            p_oDetail.setMetaData(meta);
            
            p_nEditMode = 0;
        } catch (SQLException e) {
            e.printStackTrace();
            p_sMessage = e.getMessage();
            return false;
        }
        
        return true;
    }
    
    private String getSQ_Master(){
        return "SELECT" +
                    "  a.sClientID" +
                    ", a.sBranchCd" +
                    ", a.nCredLimt" +
                    ", a.nABalance" +
                " FROM " + MASTER_TABLE + " a" +
                " WHERE a.sBranchCd = " + SQLUtil.toSQL(p_sBranchCd);
    }
}
