package joinequiv.percona;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import joinequiv.DBMSSpecificOptions;

import java.util.Arrays;
import java.util.List;

@Parameters(separators = "=", commandDescription = "Percona (default port: " + PerconaOptions.DEFAULT_PORT
        + ", default host: " + PerconaOptions.DEFAULT_HOST + ")")
public class PerconaOptions implements DBMSSpecificOptions<PerconaOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 3306;

    @Parameter(names = "--oracle")
    public List<PerconaOracleFactory> oracles = Arrays.asList(PerconaOracleFactory.JOIN);

    @Override
    public List<PerconaOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
