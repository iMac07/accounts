import java.sql.Date;
import java.sql.SQLException;
import org.junit.AfterClass;
import static org.junit.Assert.fail;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.xersys.accounts.APPayment;
import org.xersys.commander.base.Nautilus;
import org.xersys.commander.base.Property;
import org.xersys.commander.base.SQLConnection;
import org.xersys.commander.crypt.CryptFactory;
import org.xersys.commander.iface.LMasDetTrans;
import org.xersys.commander.util.SQLUtil;
import org.xersys.commander.util.StringUtil;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class testAPPaymentClose {
    static Nautilus _nautilus;
    static APPayment _trans;
    static LMasDetTrans _listener;
    
    public testAPPaymentClose(){}
    
    @BeforeClass
    public static void setUpClass() {        
        setupConnection();
        setupObject();
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Test
    public void test01CloseTransaction(){
        System.out.println("----------------------------------------");
        System.out.println("test01CloseTransaction() --> Start");
        System.out.println("----------------------------------------");
        
        if (_trans.OpenTransaction("000122000001")){
            System.out.println((String) _trans.getMaster("xClientNm"));
            System.out.println((String) _trans.getMaster("sRemarksx"));
            System.out.println(StringUtil.NumberFormat((Number) _trans.getMaster("nTranTotl"), "#,##0.00"));
            
            for (int lnCtr = 0; lnCtr <= _trans.getItemCount()-1; lnCtr++){
                System.out.println(lnCtr + 1);
                System.out.println((String) _trans.getDetail(lnCtr, "sSourceCd"));
                System.out.println((String) _trans.getDetail(lnCtr, "sSourceNo"));
                System.out.println(StringUtil.NumberFormat((Number) _trans.getDetail(lnCtr, "nDebitAmt"), "#,##0.00"));
                System.out.println(StringUtil.NumberFormat((Number) _trans.getDetail(lnCtr, "nCredtAmt"), "#,##0.00"));
                System.out.println(StringUtil.NumberFormat((Number) _trans.getDetail(lnCtr, "nAppliedx"), "#,##0.00"));
                System.out.println((String) _trans.getDetail(lnCtr, "xDescript"));
                System.out.println((String) _trans.getDetail(lnCtr, "xReferNox"));
                System.out.println(SQLUtil.dateFormat(_trans.getDetail(lnCtr, "dTransact"), SQLUtil.FORMAT_MEDIUM_DATE));
                System.out.println(SQLUtil.dateFormat(_trans.getDetail(lnCtr, "dDueDatex"), SQLUtil.FORMAT_MEDIUM_DATE));
            }
            
            if (!_trans.CloseTransaction("mac")) fail(_trans.getMessage());
        } else {
            fail(_trans.getMessage());
        }

        System.out.println("----------------------------------------");
        System.out.println("test01CloseTransaction() --> End");
        System.out.println("----------------------------------------");
    }
    
    
    private static void setupConnection(){
        String PRODUCTID = "Daedalus";
        
        //get database property
        Property loConfig = new Property("db-config.properties", PRODUCTID);
        if (!loConfig.loadConfig()){
            System.err.println(loConfig.getMessage());
            System.exit(1);
        } else System.out.println("Database configuration was successfully loaded.");
        
        //connect to database
        SQLConnection loConn = new SQLConnection();
        loConn.setProperty(loConfig);
        if (loConn.getConnection() == null){
            System.err.println(loConn.getMessage());
            System.exit(1);
        } else
            System.out.println("Connection was successfully initialized.");        
        
        //load application driver
        _nautilus = new Nautilus();
        
        _nautilus.setConnection(loConn);
        _nautilus.setEncryption(CryptFactory.make(CryptFactory.CrypType.AESCrypt));
        
        _nautilus.setUserID("0001210001");
        if (!_nautilus.load(PRODUCTID)){
            System.err.println(_nautilus.getMessage());
            System.exit(1);
        } else
            System.out.println("Application driver successfully initialized.");
    }
    
    private static void setupObject(){
        _listener = new LMasDetTrans() {          
            @Override
            public void MasterRetreive(int fnIndex, Object foValue) {
                System.out.println(fnIndex + " ->> " + foValue);
            }
            
            @Override
            public void DetailRetreive(int fnRow, int fnIndex, Object foValue) {
                System.out.println(fnRow + " ->> " + fnIndex + " ->> " + foValue);
            }
            
            @Override
            public void MasterRetreive(String fsFieldNm, Object foValue) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void DetailRetreive(int fnRow, String fsFieldNm, Object foValue) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        };
        
        _trans = new APPayment(_nautilus, (String) _nautilus.getBranchConfig("sBranchCd"), false);
        _trans.setListener(_listener);
        _trans.setWithUI(false);
    }
}
