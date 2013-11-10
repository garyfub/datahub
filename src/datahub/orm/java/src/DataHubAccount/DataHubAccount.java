package DataHubAccount;

public class DataHubAccount {
	
	private String accountId;
	private String apiKey;
	
	public DataHubAccount(String accountId, String apiKey){
		this.accountId = accountId;
		this.apiKey = apiKey;
	}
	public String getAccountId(){
		return this.accountId;
	}
	public String getApiKey(){
		return this.apiKey;
	}
	public void setAccountId(String accountId){
		this.accountId = accountId;
	}
	public void setApiKey(String apiKey){
		this.apiKey = apiKey;
	}
}
