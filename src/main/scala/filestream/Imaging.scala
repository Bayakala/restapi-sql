package com.datatech.sdp.file
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.awt.AlphaComposite
import javax.imageio.ImageIO
import java.awt.geom.AffineTransform
import java.awt.image.AffineTransformOp
import com.sun.image.codec.jpeg.JPEGCodec
import javax.swing.ImageIcon
import java.awt.Color
import java.awt.Image
import java.awt.image.BufferedImage
import java.awt.image.ConvolveOp
import java.awt.image.Kernel

object Imaging {
  def setImageSize(barr: Array[Byte], width: Int, height: Int): Array[Byte] = {
    val originalImage: BufferedImage = ImageIO.read(new ByteArrayInputStream(barr))
    // Resize
    val resized = originalImage.getScaledInstance(width, height, Image.SCALE_DEFAULT)

    // Saving Image back to disck
    val bufferedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
    bufferedImage.getGraphics.drawImage(resized, 0, 0, null)

    val barros = new ByteArrayOutputStream()
    ImageIO.write(bufferedImage, "JPEG", barros)
    barros.toByteArray
  }

  def resizeImage(barr: Array[Byte], maxWidth:Int, maxHeight:Int): Array[Byte] = {
    require (maxWidth > 0)
    require (maxHeight > 0)
    val originalImage:BufferedImage = ImageIO.read(new ByteArrayInputStream(barr))
    var height = originalImage.getHeight
    var width = originalImage.getWidth

    // Shortcut to save a pointless reprocessing in case the image is small enough already
    if (width <= maxWidth && height <= maxHeight)
      barr
    else {
      // If the picture was too big, it will either fit by width or height.
      // This essentially resizes the dimensions twice, until it fits
      if (width > maxWidth){
        height = (height.doubleValue() * (maxWidth.doubleValue() / width.doubleValue())).intValue
        width = maxWidth
      }
      if (height > maxHeight){
        width = (width.doubleValue() * (maxHeight.doubleValue() / height.doubleValue())).intValue
        height = maxHeight
      }
      val scaledBI = new BufferedImage(width, height,  BufferedImage.TYPE_INT_RGB)
      val g = scaledBI.createGraphics
      g.setComposite(AlphaComposite.Src)
      g.drawImage(originalImage, 0, 0, width, height, null);
      g.dispose
      val barros = new ByteArrayOutputStream()
      ImageIO.write(scaledBI, "JPEG", barros)
      barros.toByteArray
    }
  }

  def zoomImage(barr: Array[Byte], w:Int, h:Int): Array[Byte] = {
    val bufImg:BufferedImage = ImageIO.read(new ByteArrayInputStream(barr))
    val tmpImg: Image = bufImg.getScaledInstance(w, h, Image.SCALE_SMOOTH) //设置缩放目标图片模板
    val wr = w * 1.0 / bufImg.getWidth //获取缩放比例
    val hr = h * 1.0 / bufImg.getHeight
    val ato = new AffineTransformOp(AffineTransform.getScaleInstance(wr, hr), null)
    val imgFinal: Image = ato.filter(bufImg, null)
    val barros = new ByteArrayOutputStream()
    ImageIO.write(imgFinal.asInstanceOf[BufferedImage], "JPEG", barros)
    barros.toByteArray
  }


  def imageResize(barr: Array[Byte], maxWidth: Int, maxHeight: Int, quality: Double = 0.9): Array[Byte] = {
    require(maxWidth > 0)
    require(maxHeight > 0)
    require(quality >= 0.1)
    require(quality <= 1.0)
    var resizedImage: Image = null
    val orgImg: BufferedImage = ImageIO.read(new ByteArrayInputStream(barr))
    val iWidth = orgImg.getWidth(null)
    val iHeight = orgImg.getHeight(null)
    var newWidth = maxWidth
    if (iWidth < maxWidth) newWidth = iWidth
    if (iWidth >= iHeight)
      resizedImage = orgImg.getScaledInstance(newWidth, (newWidth * iHeight) / iWidth, Image.SCALE_SMOOTH)
    var newHeight = maxHeight
    if (iHeight < maxHeight) newHeight = iHeight
    if (resizedImage == null && iHeight >= iWidth)
      resizedImage = orgImg.getScaledInstance((newHeight * iWidth) / iHeight, newHeight, Image.SCALE_SMOOTH)
    // This code ensures that all the pixels in the image are loaded.
    val temp = new ImageIcon(resizedImage).getImage
    // Create the buffered image.
    var bufferedImage = new BufferedImage(temp.getWidth(null), temp.getHeight(null), BufferedImage.TYPE_INT_RGB)
    // Copy image to buffered image.
    val g = bufferedImage.createGraphics
    // Clear background and paint the image.
    g.setColor(Color.white)
    g.fillRect(0, 0, temp.getWidth(null), temp.getHeight(null))
    g.drawImage(temp, 0, 0, null)
    g.dispose
    // Soften.
    val softenFactor = 0.05f
    val softenArray = Array(0, softenFactor, 0, softenFactor, 1 - (softenFactor * 4), softenFactor, 0, softenFactor, 0)
    val kernel = new Kernel(3, 3, softenArray)
    val cOp = new ConvolveOp(kernel, ConvolveOp.EDGE_NO_OP, null)
    bufferedImage = cOp.filter(bufferedImage, null)
    // Write the jpeg to a file.
    val barros = new ByteArrayOutputStream()
    // Encodes image as a JPEG data stream
    val encoder = JPEGCodec.createJPEGEncoder(barros)
    val param = encoder.getDefaultJPEGEncodeParam(bufferedImage)
    param.setQuality(quality.asInstanceOf[Float], true)
    encoder.setJPEGEncodeParam(param)
    encoder.encode(bufferedImage)
    barros.toByteArray
  }

}
