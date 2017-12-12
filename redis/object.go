package redis

func (c *client) getStringOrReply(i interface{}, emptymsg []byte, errmsg []byte) ([]byte, bool) {
	if i == nil {
		if emptymsg != nil {
			c.addReply(emptymsg)
		}
		return nil, true
	}
	switch v := i.(type) {
	case []byte:
		return v, true
	default:
		if errmsg != nil {
			c.addReply(errmsg)
		}
		return nil, false
	}
}
