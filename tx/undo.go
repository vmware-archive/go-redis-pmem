package tx

import (
	"reflect"
	"log"
)

func UpdateUndo(dst, src interface{}) {
	vdst := reflect.ValueOf(dst)
	vsrc := reflect.ValueOf(src)

	/* Get the underlying value of dst. */
	switch kind := vdst.Kind(); kind {
	  	case reflect.Array: 
	  	case reflect.Slice:
	  	case reflect.Ptr:		
	  		vdst = vdst.Elem()
	  	default:
	  		log.Fatal("tx.UpdateUndo: Dst must be a pointer/array/slice!")
  	}

	/*
	 * Src could be either a copy of data or a pointer to data.
	 * UpdateUndo(&struct1, struct2) or UpdateUndo(&struct1, &struct2)
	 */
	if vsrc.Kind() == reflect.Ptr && 
		vsrc.Type() != vdst.Type() {
		vsrc = vsrc.Elem()
	} 
	/* May perform a check here if do not want to panic at runtime.
	if src != nil && vsrc.Type() != vdst.Type() {
	}
	*/

	/* logging. */

	/* fence. */

	/* 
	 * Copy src to dst.
	 * reflect.Set will panic at runtime if type does not match.
	 * Need special treat for nil values.
	 * Current slice/array copy is not efficient.
	 */
	if src == nil {
		vdst.Set(reflect.Zero(vdst.Type()))
		return
	}
	switch kind := vdst.Kind(); kind {
	  	case reflect.Array: fallthrough
	  	case reflect.Slice:	  		
  			for i := 0; i < vdst.Len(); i++ {
  				vdst.Index(i).Set(vsrc.Index(i))
  			}
	  	default:
	  		vdst.Set(vsrc)
  	}

  	/* flushing. */
}