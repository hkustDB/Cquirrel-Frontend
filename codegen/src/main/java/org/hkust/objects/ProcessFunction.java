package org.hkust.objects;

import org.hkust.checkerutils.CheckerUtils;
import org.jetbrains.annotations.Nullable;

import java.util.List;


public abstract class ProcessFunction {
    private final String name;
    @Nullable
    private final List<String> thisKey;
    @Nullable
    private final List<String> nextKey;

    public ProcessFunction(final String name, final List<String> thisKey, final List<String> nextKey) {
        CheckerUtils.checkNullOrEmpty(name, "name");
        CheckerUtils.validateNonNullNonEmpty(thisKey, "thisKey");
        CheckerUtils.validateNonNullNonEmpty(nextKey, "nextKey");

        this.name = name;
        this.thisKey = thisKey;
        this.nextKey = nextKey;
    }

    public abstract String getName();
}
