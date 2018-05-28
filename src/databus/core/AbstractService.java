package databus.core;

/**
 * Created by Qu Chunhe on 2018-05-28.
 */
public abstract class AbstractService extends RunnerHolder implements Service {
    public AbstractService(Runner runner, String name) {
        super(runner, name);
    }

    public AbstractService(Runner runner) {
        super(runner);
    }

    public AbstractService() {
    }
}
