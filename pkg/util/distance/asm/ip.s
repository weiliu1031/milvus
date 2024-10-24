// Code generated by command: go run ip.go -out ip.s -stubs ip_stub.go. DO NOT EDIT.

#include "textflag.h"

// func IP(x []float32, y []float32) float32
// Requires: AVX, FMA3, SSE
TEXT ·IP(SB), NOSPLIT, $0-52
	MOVQ   x_base+0(FP), AX
	MOVQ   y_base+24(FP), CX
	MOVQ   x_len+8(FP), DX
	VXORPS Y0, Y0, Y0
	VXORPS Y1, Y1, Y1
	VXORPS Y2, Y2, Y2
	VXORPS Y3, Y3, Y3

blockloop:
	CMPQ        DX, $0x00000020
	JL          tail
	VMOVUPS     (AX), Y4
	VMOVUPS     32(AX), Y5
	VMOVUPS     64(AX), Y6
	VMOVUPS     96(AX), Y7
	VFMADD231PS (CX), Y4, Y0
	VFMADD231PS 32(CX), Y5, Y1
	VFMADD231PS 64(CX), Y6, Y2
	VFMADD231PS 96(CX), Y7, Y3
	ADDQ        $0x00000080, AX
	ADDQ        $0x00000080, CX
	SUBQ        $0x00000020, DX
	JMP         blockloop

tail:
	VXORPS X4, X4, X4

tailloop:
	CMPQ        DX, $0x00000000
	JE          reduce
	VMOVSS      (AX), X5
	VFMADD231SS (CX), X5, X4
	ADDQ        $0x00000004, AX
	ADDQ        $0x00000004, CX
	DECQ        DX
	JMP         tailloop

reduce:
	VADDPS       Y0, Y1, Y0
	VADDPS       Y2, Y3, Y2
	VADDPS       Y0, Y2, Y0
	VEXTRACTF128 $0x01, Y0, X1
	VADDPS       X0, X1, X0
	VADDPS       X0, X4, X0
	VHADDPS      X0, X0, X0
	VHADDPS      X0, X0, X0
	MOVSS        X0, ret+48(FP)
	RET
