// @@@SNIPSTART money-transfer-java-transaction-details
package ai.chronon.orchestration.temporal;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(as = CoreTransactionDetails.class)
public interface TransactionDetails {
    String getSourceAccountId();
    String getDestinationAccountId();
    String getTransactionReferenceId();
    int getAmountToTransfer();
}


