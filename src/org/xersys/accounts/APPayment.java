package org.xersys.accounts;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.xersys.accounts.client.APClientTrans;
import org.xersys.clients.search.ClientSearch;
import org.xersys.commander.contants.EditMode;
import org.xersys.commander.contants.TransactionStatus;
import org.xersys.commander.iface.LMasDetTrans;
import org.xersys.commander.iface.XNautilus;
import org.xersys.commander.util.MiscUtil;
import org.xersys.commander.util.SQLUtil;
import org.xersys.commander.util.StringUtil;

public class APPayment {
    private static final String MASTER_TABLE = "AP_Payment_Master";
    private static final String DETAIL_TABLE = "AP_Payment_Detail";
    
    private final XNautilus p_oNautilus;
    private final String p_sBranchCd;
    private final boolean p_bWithParent;
    
    private LMasDetTrans p_oListener;
    private boolean p_bWithUI = true;
    
    private int p_nEditMode;
    private int p_nTranStat;
    
    private String p_sMessage;
    
    private ClientSearch p_oClient;
    
    private CachedRowSet p_oMaster;
    private CachedRowSet p_oDetail;
    
    public APPayment(XNautilus foNautilus, String fsBranchCd, boolean fbWithParent){
        p_oNautilus = foNautilus;
        p_sBranchCd = fsBranchCd;
        p_bWithParent = fbWithParent;
        
        p_oClient = new ClientSearch(p_oNautilus, ClientSearch.SearchType.searchAPClient);
        
        p_nEditMode = EditMode.UNKNOWN;
    }
    
    public int getEditMode(){
        return p_nEditMode;
    }
    
    public void setListener(LMasDetTrans foValue) {
        p_oListener = foValue;
    }
    
    public void setWithUI(boolean fbValue){
        p_bWithUI = fbValue;
    }

    public String getMessage(){
        return p_sMessage;
    }
    
    private void setMessage(String fsValue){
        p_sMessage = fsValue;
    }
    
    public int getItemCount() {
        try {
            p_oDetail.last();
            return p_oDetail.getRow();
        } catch (SQLException e) {
            e.printStackTrace();
            setMessage(e.getMessage());
            return -1;
        }
    }
    
    public boolean NewTransaction(){
        System.out.println(this.getClass().getSimpleName() + ".NewTransaction()");
        
        try {
            String lsSQL;
            ResultSet loRS;
            
            RowSetFactory factory = RowSetProvider.newFactory();
            
            //create empty master record
            lsSQL = MiscUtil.addCondition(getSQ_Master(), "0=1");
            loRS = p_oNautilus.executeQuery(lsSQL);
            p_oMaster = factory.createCachedRowSet();
            p_oMaster.populate(loRS);
            MiscUtil.close(loRS);
            addMasterRow();
        } catch (SQLException ex) {
            setMessage(ex.getMessage());
            return false;
        }
        
        p_nEditMode = EditMode.ADDNEW;
        
        return true;
    }
    
    public boolean LoadTransaction(){
        if (p_nEditMode != EditMode.ADDNEW) {
            setMessage("Invalid Edit Mode Detected.");
            return false;
        }
        
        if ("".equals(getMaster("sClientID"))){
            setMessage("No supplier was selected.");
            return false;
        }

        try {
            String lsSQL;
            ResultSet loRS;
            RowSetFactory factory = RowSetProvider.newFactory();
            
            //create empty detail record
            lsSQL = MiscUtil.addCondition(getSQ_Detail(), "0=1");
            loRS = p_oNautilus.executeQuery(lsSQL);
            p_oDetail = factory.createCachedRowSet();
            p_oDetail.populate(loRS);
            MiscUtil.close(loRS);
            
            lsSQL = "SELECT" + 
                        "  CONCAT(b.sInvTypCd, ' Purchase') xDescript" + 
                        ", b.sReferNox" + 
                        ", DATE_FORMAT(b.dTransact, '%Y-%m-%d') dTransact" + 
                        ", DATE_FORMAT(b.dRefernce, '%Y-%m-%d') dRefernce" + 
                        ", DATE_ADD(DATE_FORMAT(b.dRefernce, '%Y-%m-%d'), INTERVAL IFNULL(c.nTermValx, 0) DAY) dDueDatex" + 
                        ", DATEDIFF(NOW(), DATE_ADD(DATE_FORMAT(b.dRefernce, '%Y-%m-%d'), INTERVAL IFNULL(c.nTermValx, 0) DAY)) nAgexxxxx" +
                        ", a.nCreditxx nDebitxxx" + 
                        ", a.nDebitxxx - b.nAmtPaidx nCreditxx" + 
                        ", a.nDebitxxx nAppliedx" + 
                        ", a.sSourceNo sTransNox" + 
                        ", a.sSourceCd" + 
                        ", a.sClientID" + 
                    " FROM AP_Ledger a" + 
                        ", PO_Receiving_Master b" + 
                            " LEFT JOIN Term c ON b.sTermCode = c.sTermCode" + 
                    " WHERE a.sSourceCd = 'DA'" + 
                        " AND a.sClientID = " + SQLUtil.toSQL((String) getMaster("sClientID")) +
                        " AND a.sSourceNo = b.sTransNox" + 
                        " AND (a.nCreditxx - b.nAmtPaidx) > 0.00" + 
                        " AND b.cTranStat <> '3'" + 
                    " UNION SELECT "+ 
                        "  CONCAT(b.sInvTypCd, ' Purchase Return') xDescript" + 
                        ", b.sTransNox sReferNox" + 
                        ", DATE_FORMAT(b.dTransact, '%Y-%m-%d') dTransact" + 
                        ", DATE_FORMAT(b.dTransact, '%Y-%m-%d') dRefernce" +
                        ", DATE_FORMAT(b.dTransact, '%Y-%m-%d') dDueDatex" + 
                        ", DATEDIFF(NOW(), DATE_FORMAT(b.dTransact, '%Y-%m-%d')) nAgexxxxx" +
                        ", a.nCreditxx nDebitxxx" + 
                        ", a.nDebitxxx - b.nAmtPaidx nCreditxx" + 
                        ", a.nDebitxxx nAppliedx" + 
                        ", a.sSourceNo sTransNox" + 
                        ", a.sSourceCd" + 
                        ", a.sClientID" + 
                        " FROM AP_Ledger a" + 
                            ", PO_Return_Master b" + 
                        " WHERE a.sSourceCd = 'PR'" + 
                            " AND a.sClientID = " + SQLUtil.toSQL((String) getMaster("sClientID")) +
                            " AND a.sSourceNo = b.sTransNox" + 
                            " AND a.dPostedxx IS NULL" + 
                            " AND b.cTranStat <> '3'";

            loRS = p_oNautilus.executeQuery(lsSQL);
            
            while (loRS.next()){
                addDetail();
                setDetail(getItemCount() - 1, "sSourceCd", loRS.getString("sSourceCd"));
                setDetail(getItemCount() - 1, "sSourceNo", loRS.getString("sTransNox"));
                setDetail(getItemCount() - 1, "nDebitAmt", loRS.getDouble("nDebitxxx"));
                setDetail(getItemCount() - 1, "nCredtAmt", loRS.getDouble("nCreditxx"));
                setDetail(getItemCount() - 1, "nAppliedx", 0.00);
                setDetail(getItemCount() - 1, "xDescript", loRS.getString("xDescript"));
                setDetail(getItemCount() - 1, "dTransact", loRS.getDate("dTransact"));
                setDetail(getItemCount() - 1, "dRefernce", loRS.getDate("dRefernce"));
                setDetail(getItemCount() - 1, "dDueDatex", loRS.getDate("dDueDatex"));
                setDetail(getItemCount() - 1, "xReferNox", loRS.getString("sReferNox"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            setMessage(e.getMessage());
            return false;
        }
        
        return true;
    }
    
    public boolean SaveTransaction(){
        try {
            if (p_nEditMode != EditMode.ADDNEW) {
                setMessage("Invalid Edit Mode Detected.");
                return false;
            }

            if ("".equals(getMaster("sClientID"))){
                setMessage("No supplier was selected.");
                return false;
            }
            
            String lsSQL;
            
            p_oMaster.updateObject("sTransNox", MiscUtil.getNextCode(MASTER_TABLE, "sTransNox", true, p_oNautilus.getConnection().getConnection(), p_sBranchCd));
            p_oMaster.updateObject("sBranchCd", p_sBranchCd);
            p_oMaster.updateObject("dTransact", p_oNautilus.getServerDate());
            p_oMaster.updateObject("dModified", p_oNautilus.getServerDate());
            p_oMaster.updateRow();
            
            int lnCtr = 0;
            double lnTranTotl = 0.00;
            
            p_oNautilus.beginTrans();
            
            for (int lnRow = 0; lnRow <= getItemCount() - 1; lnRow++){
                if ((double) getDetail(lnRow, "nAppliedx") != 0.00){
                    p_oDetail.updateObject("sTransNox", getMaster("sTransNox"));
                    p_oDetail.updateObject("nEntryNox", lnCtr + 1);
                    p_oDetail.updateRow();
                    
                    lsSQL = MiscUtil.rowset2SQL(p_oDetail, DETAIL_TABLE, "xDescript;dTransact;xReferNox;dDueDatex");
                    
                    if(p_oNautilus.executeUpdate(lsSQL, DETAIL_TABLE, p_sBranchCd, "") <= 0){
                        System.err.println(p_oNautilus.getMessage());
                        p_oNautilus.rollbackTrans();
                        p_sMessage = "Unable to update PAYMENT DETAIL.";
                        return false;
                    } 
                    
                    if (!"PR".equals((String) getDetail(lnRow, "sSourceCd"))) lnTranTotl += (double) getDetail(lnRow, "nAppliedx");
                    
                    lnCtr++;
                }
            }
            
            if (lnCtr == 0){
                p_oNautilus.rollbackTrans();
                p_sMessage = "No PAYMENT DETAIL to save.";
                return false;
            }
            
            p_oMaster.updateObject("nTranTotl", lnTranTotl);
            p_oMaster.updateObject("nEntryNox", lnCtr);
            p_oMaster.updateRow();
            
            lsSQL = MiscUtil.rowset2SQL(p_oMaster, MASTER_TABLE, "sClientNm");
            
            if(p_oNautilus.executeUpdate(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0){
                System.err.println(p_oNautilus.getMessage());
                p_oNautilus.rollbackTrans();
                p_sMessage = "Unable to update PAYMENT MASTER";
                return false;
            } 
            
            p_oNautilus.commitTrans();
        } catch (SQLException e) {
            e.printStackTrace();
            p_oNautilus.rollbackTrans();
            p_sMessage = e.getMessage();
            return false;
        }
        
        return true;
    }
    
    public boolean OpenTransaction(String fsTransNox){
       System.out.println(this.getClass().getSimpleName() + ".OpenTransaction(String fsTransNox)");
        
        try {
            String lsSQL;
            ResultSet loRS;
            
            RowSetFactory factory = RowSetProvider.newFactory();
            
            //create empty master record
            lsSQL = MiscUtil.addCondition(getSQ_Master(), "a.sTransNox = " + SQLUtil.toSQL(fsTransNox)); 
            loRS = p_oNautilus.executeQuery(lsSQL);
            
            if (MiscUtil.RecordCount(loRS) == 0){
                p_sMessage = "No record found.";
                return false;
            }

            p_oMaster = factory.createCachedRowSet();
            p_oMaster.populate(loRS);
            MiscUtil.close(loRS);
            
             //create empty detail record
            lsSQL = MiscUtil.addCondition(getSQ_Detail(), "a.sTransNox = " + SQLUtil.toSQL(fsTransNox));
            loRS = p_oNautilus.executeQuery(lsSQL);
            p_oDetail = factory.createCachedRowSet();
            p_oDetail.populate(loRS);
            MiscUtil.close(loRS);
            
            p_oMaster.first();
            p_oDetail.last();
            
            if (p_oMaster.getInt("nEntryNox") != p_oDetail.getRow()){
                p_sMessage = "Transaction discrepancy detected.";
                
                p_oMaster = null;
                p_oDetail = null;
                
                return false;
            }
        } catch (SQLException ex) {
            p_oMaster = null;
            p_oDetail = null;
            setMessage(ex.getMessage());
            return false;
        }
        
        p_nEditMode = EditMode.READY;
        
        return true;
    }
            
    public boolean CloseTransaction(String fsAprvCode){
        if (p_nEditMode != EditMode.READY){
            p_sMessage = "Invalid Edit Mode Detected.";
            return false;
        }
        
        if (!"0".equals(getMaster("cTranStat"))){
            p_sMessage = "Unable to approve already processed transaction.";
            return false;
        }
        
        try {            
            APClientTrans loClient = new APClientTrans(p_oNautilus, p_sBranchCd);
            
            for(int lnRow = 0; lnRow <= getItemCount() - 1; lnRow++){
                if ("PR".equals((String) getDetail(lnRow, "sSourceCd"))){
                    loClient.AddDetail((String) getDetail(lnRow, "sSourceCd"), 
                                        (String) getDetail(lnRow, "sSourceNo"), 
                                        (Date) getMaster("dTransact"), 
                                        Double.valueOf(String.valueOf(getDetail(lnRow, "nAppliedx"))),
                                        0.00);
                } else {
                    loClient.AddDetail((String) getDetail(lnRow, "sSourceCd"), 
                                        (String) getDetail(lnRow, "sSourceNo"), 
                                        (Date) getMaster("dTransact"), 
                                        0.00, 
                                        Double.valueOf(String.valueOf(getDetail(lnRow, "nAppliedx"))));
                }
            }
            
            p_oNautilus.beginTrans();
            
            if (!loClient.PaymentIssue((String) getMaster("sTransNox"), 
                                    (String) getMaster("sClientID"), 
                                    (Date) getMaster("dTransact"), 
                                    0.00, 
                                    Double.valueOf(String.valueOf(getMaster("nTranTotl"))), 
                                    EditMode.ADDNEW)){
                p_oNautilus.rollbackTrans();
                p_sMessage = loClient.getMessage();
                return false;
            }
            
            if (Double.valueOf(String.valueOf(getMaster("nTranTotl"))) > 0.00){
                p_oMaster.updateObject("nCheckAmt", getMaster("nTranTotl"));
            }
            
            p_oMaster.updateObject("cTranStat", TransactionStatus.STATE_CLOSED);
            p_oMaster.updateRow();
            
            String lsSQL = "UPDATE " + MASTER_TABLE + " SET" +
                                "  cTranStat = '1'" +
                                ", nCheckAmt = " + Double.valueOf(String.valueOf(getMaster("nTranTotl"))) +
                                ", sAprvCode = " + SQLUtil.toSQL(fsAprvCode.trim()) +
                                ", dModified = " + SQLUtil.toSQL(p_oNautilus.getServerDate()) +
                            " WHERE sTransNox = " + SQLUtil.toSQL((String) getMaster("sTransNox"));
            
            if(p_oNautilus.executeUpdate(lsSQL, MASTER_TABLE, p_sBranchCd, "") <= 0){
                System.err.println(p_oNautilus.getMessage());
                p_oNautilus.rollbackTrans();
                p_sMessage = "Unable to update PAYMENT MASTER";
                return false;
            } 
            
            p_oNautilus.commitTrans();
        } catch (SQLException e) {
            e.printStackTrace();
            p_sMessage = e.getMessage();
            return false;
        }
        
        return true;
    }
    
    private boolean addDetail() {
        try {
//            if (getItemCount() > 0) {
//                if ("".equals((String) getDetail(getItemCount() - 1, "xReferNox"))){
//                    return true;
//                }
//            }
            
            p_oDetail.last();
            p_oDetail.moveToInsertRow();

            MiscUtil.initRowSet(p_oDetail);

            p_oDetail.insertRow();
            p_oDetail.moveToCurrentRow();
        } catch (SQLException e) {
            setMessage(e.getMessage());
            return false;
        }

        return true;
    }
    
    public void setMaster(String fsFieldNm, Object foValue) {
        try {
            setMaster(MiscUtil.getColumnIndex(p_oMaster, fsFieldNm), foValue);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    public void setMaster(int fnIndex, Object foValue) {
        try {
            p_oMaster.first();
            
            switch (fnIndex){
                case 3: //sClientID
                    getMaster(fnIndex, foValue);
                    break;
                case 4: //dTransact
                    if (foValue instanceof Date){
                        p_oMaster.updateObject(fnIndex, (Date) foValue);
                        p_oMaster.updateRow();
                    }
                    
                    if (p_oListener != null) p_oListener.MasterRetreive(fnIndex, getMaster(fnIndex));
                    break;
                case 5: //sRemarksx
                case 10: //sAprvCode
                    p_oMaster.updateString(fnIndex, (String) foValue);
                    p_oMaster.updateRow();

                    if (p_oListener != null) p_oListener.MasterRetreive(fnIndex, getMaster(fnIndex));
                    break;
            }
        } catch (ParseException | SQLException e) {
            e.printStackTrace();
        }
    }
    
    public void setDetail(int fnRow, String fsFieldNm, Object foValue) {
        try {
            setDetail(fnRow, MiscUtil.getColumnIndex(p_oDetail, fsFieldNm), foValue);
        } catch (SQLException e) {
            e.printStackTrace();;
        }
    }
    
    public void setDetail(int fnRow, int fnIndex, Object foValue) {
        try {           
            if (getItemCount() <= 0 || fnRow + 1 <= 0){
                System.err.println("Invalid row index!");
                return;
            }
            
            p_oDetail.absolute(fnRow + 1);
            
            switch (fnIndex){
                case 1: //sTransNox
                case 2: //nEntryNox
                case 8: //dModified
                    break;
                case 10: //dTransact
                case 11: //dRefernce
                case 12: //dDueDatex
                    if (foValue instanceof Date){
                        p_oDetail.updateObject(fnIndex, (Date) foValue);
                    }
                    break;
                case 3: //sSourceCd
                case 4: //sSourceNo
                case 9: //xDescript
                case 13: //xReferNox
                    p_oDetail.updateObject(fnIndex, String.valueOf(foValue));
                    break;
                case 5: //nDebitAmt
                case 6: //nCredtAmt        
                    if (!StringUtil.isNumeric(String.valueOf(foValue))) 
                        p_oDetail.updateObject(fnIndex, 0.00);
                    else
                        p_oDetail.updateObject(fnIndex, (double) foValue);
                    
                    p_oDetail.updateRow();
                    break;
                case 7: //nAppliedx                    
                    if (!StringUtil.isNumeric(String.valueOf(foValue))) 
                        p_oDetail.updateObject(fnIndex, 0.00);
                    else{
                        if (p_oDetail.getString("sSourceCd").equals("PR")){
                            if ((double) foValue > 0.00)
                                p_oDetail.updateObject(fnIndex, (double) getDetail(fnRow, "nCredtAmt"));
                            else
                                p_oDetail.updateObject(fnIndex, 0.00);
                        } else {
                            double lnMaxAmnt = (double) getDetail(fnRow, "nDebitAmt");
                            
                            if (lnMaxAmnt < (double) foValue)
                                p_oDetail.updateObject(fnIndex, lnMaxAmnt);
                            else
                                p_oDetail.updateObject(fnIndex, (double) foValue);
                        }
                    }
                    p_oDetail.updateRow();
                    
                    computeTotal();
                    break;
            }
            
            if (p_oListener != null) p_oListener.DetailRetreive(fnRow, fnIndex, getDetail(fnRow, fnIndex));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    public Object getMaster(int fnIndex) {
        try {
            p_oMaster.first();
            return p_oMaster.getObject(fnIndex);
        } catch (SQLException e) {
            e.printStackTrace();
            setMessage("SQL Exception!");
        }
        
        return null;
    }
    
    public Object getMaster(String fsFieldNm) {
        try {
            return getMaster(MiscUtil.getColumnIndex(p_oMaster, fsFieldNm));
        } catch (SQLException e) {
            e.printStackTrace();
            setMessage("SQL Exception!");
        }
        
        return null;
    }
    
    public Object getDetail(int fnRow, int fnIndex) {        
        try {
            if (getItemCount() <= 0 || fnRow + 1 <= 0){
                setMessage("Invalid row index!");
                return null;
            }
            
            p_oDetail.absolute(fnRow + 1);            
            return p_oDetail.getObject(fnIndex);
        } catch (SQLException e) {
            e.printStackTrace();
            setMessage("SQL Exception!");
            return null;
        }
    }
    
    public Object getDetail(int fnRow, String fsFieldNm) {
        try {
            return getDetail(fnRow, MiscUtil.getColumnIndex(p_oDetail, fsFieldNm));
        } catch (SQLException e) {
            e.printStackTrace();
            setMessage("SQL Exception!");
            return null;
        }
    }
    
    public void displayMasFields() throws SQLException{
        if (p_nEditMode != EditMode.ADDNEW && p_nEditMode != EditMode.UPDATE) return;
        
        int lnRow = p_oMaster.getMetaData().getColumnCount();
        
        System.out.println("----------------------------------------");
        System.out.println("MASTER TABLE INFO");
        System.out.println("----------------------------------------");
        System.out.println("Total number of columns: " + lnRow);
        System.out.println("----------------------------------------");
        
        for (int lnCtr = 1; lnCtr <= lnRow; lnCtr++){
            System.out.println("Column index: " + (lnCtr) + " --> Label: " + p_oMaster.getMetaData().getColumnLabel(lnCtr));
            if (p_oMaster.getMetaData().getColumnType(lnCtr) == Types.CHAR ||
                p_oMaster.getMetaData().getColumnType(lnCtr) == Types.VARCHAR){
                
                System.out.println("Column index: " + (lnCtr) + " --> Size: " + p_oMaster.getMetaData().getColumnDisplaySize(lnCtr));
            }
        }
        
        System.out.println("----------------------------------------");
        System.out.println("END: MASTER TABLE INFO");
        System.out.println("----------------------------------------");
    }
    
    public void applyAll(boolean fbValue) throws SQLException{
        int lnRow = getItemCount();
        
        for (int lnCtr = 0; lnCtr <= lnRow-1; lnCtr++){
            if (fbValue){
                setDetail(lnCtr, "nAppliedx", 0.00);
            } else {
                if ("PR".equals((String) getDetail(lnCtr, "sSourceCd")))
                    setDetail(lnCtr, "nAppliedx", (double) getDetail(lnCtr, "nCredtAmt"));
                else
                    setDetail(lnCtr, "nAppliedx", (double) getDetail(lnCtr, "nDebitAmt"));
            }
        }
        
        computeTotal();
    }
    
    private void computeTotal() throws SQLException{
        int lnRow = getItemCount();
        double lnTranTotl = 0.00;
        
        for (int lnCtr = 0; lnCtr <= lnRow-1; lnCtr++){
            if ("PR".equals((String) getDetail(lnCtr, "sSourceCd")))
                lnTranTotl -= (double) getDetail(lnCtr, "nAppliedx");
            else
                lnTranTotl += (double) getDetail(lnCtr, "nAppliedx");
        }
        
        p_oMaster.first();
        p_oMaster.updateObject("nTranTotl", lnTranTotl);
        p_oMaster.updateRow();
        
        if (p_oListener != null) p_oListener.MasterRetreive(MiscUtil.getColumnIndex(p_oMaster, "nTranTotl"), (double) getMaster("nTranTotl"));
        
    }
    
    public void displayDetFields() throws SQLException{
        if (p_nEditMode != EditMode.ADDNEW && p_nEditMode != EditMode.UPDATE) return;
        
        int lnRow = p_oDetail.getMetaData().getColumnCount();
        
        System.out.println("----------------------------------------");
        System.out.println("DETAIL TABLE INFO");
        System.out.println("----------------------------------------");
        System.out.println("Total number of columns: " + lnRow);
        System.out.println("----------------------------------------");
        
        for (int lnCtr = 1; lnCtr <= lnRow; lnCtr++){
            System.out.println("Column index: " + (lnCtr) + " --> Label: " + p_oDetail.getMetaData().getColumnLabel(lnCtr));
            if (p_oDetail.getMetaData().getColumnType(lnCtr) == Types.CHAR ||
                p_oDetail.getMetaData().getColumnType(lnCtr) == Types.VARCHAR){
                
                System.out.println("Column index: " + (lnCtr) + " --> Size: " + p_oDetail.getMetaData().getColumnDisplaySize(lnCtr));
            }
        }
        
        System.out.println("----------------------------------------");
        System.out.println("END: DETAIL TABLE INFO");
        System.out.println("----------------------------------------");
    }
    
    private void addMasterRow() throws SQLException{
        p_oMaster.last();
        p_oMaster.moveToInsertRow();
        
        MiscUtil.initRowSet(p_oMaster);
        p_oMaster.updateObject("cTranStat", TransactionStatus.STATE_OPEN);
        
        p_oMaster.insertRow();
        p_oMaster.moveToCurrentRow();
    }
    
    public JSONObject searchClient(String fsKey, Object foValue, boolean fbExact){
        p_oClient.setKey(fsKey);
        p_oClient.setValue(foValue);
        p_oClient.setExact(fbExact);
        
        p_oClient.addFilter("Branch", p_sBranchCd);
        
        return p_oClient.Search();
    }
    
    public ClientSearch getSearchClient(){
        return p_oClient;
    }
    
    private void getMaster(int fnIndex, Object foValue) throws SQLException, ParseException{       
        JSONObject loJSON;
        JSONParser loParser = new JSONParser();
        
        switch(fnIndex){
            case 3: //sClientID
                loJSON = searchClient("a.sClientID", foValue, true);
                
                if ("success".equals((String) loJSON.get("result"))){
                    loJSON = (JSONObject) ((JSONArray) loParser.parse((String) loJSON.get("payload"))).get(0);
                    
                    p_oMaster.first();
                    p_oMaster.updateObject(MiscUtil.getColumnIndex(p_oMaster, "sClientID"), (String) loJSON.get("sClientID"));
                    p_oMaster.updateObject(MiscUtil.getColumnIndex(p_oMaster, "xClientNm"), (String) loJSON.get("sClientNm"));
                    p_oMaster.updateObject(MiscUtil.getColumnIndex(p_oMaster, "xCredLimt"), (double) loJSON.get("nCredLimt"));
                    p_oMaster.updateObject(MiscUtil.getColumnIndex(p_oMaster, "xABalance"), (double) loJSON.get("nABalance"));
                    p_oMaster.updateRow();
                    
                    if (p_oListener != null) p_oListener.MasterRetreive(fnIndex, getMaster("xClientNm"));
                    if (p_oListener != null) p_oListener.MasterRetreive(MiscUtil.getColumnIndex(p_oMaster, "xCredLimt"), getMaster("xCredLimt"));
                    if (p_oListener != null) p_oListener.MasterRetreive(MiscUtil.getColumnIndex(p_oMaster, "xABalance"), getMaster("xABalance"));
                }
                break;
        }
    }
    
    private String getSQ_Master(){
        return "SELECT" +
                    "  a.sTransNox" +
                    ", a.sBranchCd" +
                    ", a.sClientID" +
                    ", a.dTransact" +
                    ", a.sRemarksx" +
                    ", a.nTranTotl" +
                    ", a.nCheckAmt" +
                    ", a.nEntryNox" +
                    ", a.cTranStat" +
                    ", a.sAprvCode" +
                    ", a.dModified" +
                    ", b.sClientNm xClientNm" +
                    ", c.nCredLimt xCredLimt" +
                    ", c.nABalance xABalance" +
                " FROM " + MASTER_TABLE + " a" +
                    " LEFT JOIN Client_Master b ON a.sClientID = b.sClientID" +
                    " LEFT JOIN AP_Master c ON a.sClientID = b.sClientID AND a.sBranchCd = c.sBranchCd";
    }
    
    private String getSQ_Detail(){
        return "SELECT" +
                    "  a.sTransNox" +
                    ", a.nEntryNox" +
                    ", a.sSourceCd" +
                    ", a.sSourceNo" +
                    ", a.nDebitAmt" +
                    ", a.nCredtAmt" +
                    ", a.nAppliedx" +
                    ", a.dModified" +
                    ", (CASE a.sSourceCd" +
                        " WHEN 'DA' THEN CONCAT(b.sInvTypCd, ' Purchase')" +
                        " WHEN 'PR' THEN CONCAT(c.sInvTypCd, ' Purchase Return')" +
                        " END) xDescript" +
                    ", (CASE a.sSourceCd" +
                        " WHEN 'DA' THEN b.dTransact" +
                        " WHEN 'PR' THEN c.dTransact" +
                        " END) dTransact" +
                    ", (CASE a.sSourceCd" +
                        " WHEN 'DA' THEN b.dRefernce" +
                        " WHEN 'PR' THEN c.dTransact" +
                        " END) dRefernce" +
                    ", (CASE a.sSourceCd" +
                        " WHEN 'DA' THEN SYSDATE()" +
                        " WHEN 'PR' THEN SYSDATE()" +
                        " END) dDueDatex" +
                    ", (CASE a.sSourceCd" +
                        " WHEN 'DA' THEN b.sReferNox" +
                        " ELSE '' END) xReferNox" +
                " FROM " + DETAIL_TABLE + " a" +
                    " LEFT JOIN PO_Receiving_Master b ON a.sSourceNo = b.sTransNox AND a.sSourceCd = 'DA'" +
                    " LEFT JOIN PO_Return_Master c ON a.sSourceNo = c.sTransNox AND a.sSourceCd = 'PR'" +
                " ORDER BY a.nEntryNox";
    }
}
