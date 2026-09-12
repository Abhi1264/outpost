package com.ledger.ledger;

import java.math.BigDecimal;
import java.math.RoundingMode;

public final class Money {

	public static final int SCALE = 4;
	public static final RoundingMode ROUNDING = RoundingMode.HALF_EVEN;
	public static final BigDecimal ZERO = BigDecimal.ZERO.setScale(SCALE, ROUNDING);

	private Money() {
	}

	public static BigDecimal of(BigDecimal value) {
		return value.setScale(SCALE, ROUNDING);
	}

}
