// clwb will be ordered by sfence
TEXT ·sfence(SB),$0
	SFENCE
	RET


TEXT ·clflush(SB), $0
	MOVQ ptr+0(FP), BX
	CLFLUSH (BX)
	RET
